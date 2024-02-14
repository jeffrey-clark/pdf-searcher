import re, os, sys
import pandas as pd
import pdfplumber
import codecs
import textract
import Functions.sql_functions as sql

from multiprocessing import Pool
from functools import partial


regex_condition = r'placebo.{,6}tests*'


def scan_pdf(fp):

    with pdfplumber.open(fp) as pdf:
        page_count = len(pdf.pages)
        matched_on_pages = []
        for p in range(0, page_count):
            page = pdf.pages[p]
            text = page.extract_text()

            m = re.search(regex_condition, text.lower())

            if m:
                matched_on_pages.append(p)

        if len(matched_on_pages) > 0:
            return matched_on_pages
        else:
            return False



def scan_pdf2(fp):
    # extract text in byte format
    textract_text = textract.process(fp)
    # convert bytes to string
    text = codecs.decode(textract_text)
    text = re.sub(r"\s+", " ", text)
    m = re.search(regex_condition, text.lower())

    if m:
        return ['?']
    else:
        return False


# function to process a journal
def process_journal(table_name, journal_name):
    pdf_dir = f"Data/{journal_name}/PDFs"
    if not os.path.isdir(pdf_dir):
        return False
    issue_dirs = os.listdir(pdf_dir)
    print(f"Scanning {journal_name}")
    for i in issue_dirs:
        if not os.path.isdir(os.path.join(pdf_dir, i)):
            continue
        m = re.search(r"(\d+)_(\d+)_(.+)", i)
        try:
            year, volume, issue = m.group(1), m.group(2), m.group(3)
        except:
            print("i is:", i)
            print("m is:", m)
            raise ValueError('regex error')

        if int(year) < 2009 or int(year) > 2021:
            continue
        #print(f"  Folder: {i}")
        dir = f"{pdf_dir}/{i}"
        files = os.listdir(dir)
        for f in files:
            fp = f"{dir}/{f}"
            #print(f"    File: {f}")

            already_inserted = bool(
                sql.count_rows(table_name, [('journal_name', journal_name), ('volume', volume), ('issue', issue),
                                            ('article', f)]))
            if already_inserted:
                continue

            error = False
            page_list = False
            try:
                tups_to_skip = [('Review of Economics and Statistics', 102, "10_1162_rest_a_00846.pdf")]
                for t in tups_to_skip:
                    if t[0] == journal_name and int(t[1]) == int(volume) and t[2] == f:
                        print("      skipping...")
                        error = True
                if not error:
                    page_list = scan_pdf(fp)

                # double check with other PDF parser
                if not page_list:
                    page_list = scan_pdf2(fp)
            except:
                error = True

            match = False
            page_string = None
            if type(page_list) is list:
                if len(page_list) > 0:
                    match = True
                    page_string = ", ".join([str(x) for x in page_list])

            row = [('journal_name', journal_name), ('year', year), ('volume', volume), ('issue', issue),
                   ('article', f), ('match', int(match)), ('pages', page_string), ('error', int(error))]
            sql.insert(table_name, row)


def main(multiprocessing=True):
    # create the sql table
    table_name = 'placebo_count'
    cols = [('journal_name', 'TEXT'), ('year', 'TEXT'), ('volume', 'TEXT'), ('issue', 'TEXT'), ('article', 'TEXT'),
            ('match', 'INT'), ('pages', 'TEXT'), ('error', 'INT')]
    if not sql.check_table_exists(table_name):
        sql.create_table(table_name, cols, [('journal_name', 100), ('volume', 50), ('issue', 50), ('article', 100)])


    journal_list = os.listdir("Data")
    if not multiprocessing:
        for journal_name in journal_list:
            process_journal(table_name, journal_name)
    else:
        p = Pool(os.cpu_count())
        p.map(partial(process_journal, table_name), journal_list)
        p.close()
        p.join()


if __name__ == "__main__":
    main()
    df = sql.download('placebo_count')
    df.to_excel("results.xlsx", index=False)