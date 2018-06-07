#! /usr/bin/env python3

import datetime
import xml.etree.cElementTree as et
import logging as lg
import pandas as pd
import sys

from pathlib import Path

lg.basicConfig(level=lg.DEBUG)

def main():
    """ Bundles the various methods """

    # parsing file
    if len(sys.argv) != 2:
        lg.error('Usage: $ setup <path/to/file.gsb>')
    else:
        try:
            filepath = Path(sys.argv[1]).resolve()
        except FileNotFoundError:
            lg.error(f"File {filepath} not found.")
        else:
            lg.info(f"File {filepath} found")

             # initialize DataFrame
            currency_cols = ["id", "name", "symbol", "trigram"]
            currency_df = pd.DataFrame(columns=currency_cols)
            account_cols = ["id", "name", "currency", "initial_balance"]
            account_df = pd.DataFrame(columns=account_cols)
            # NB: we didn't create payment_df
            transaction_cols = ["id", "account", "date", "value_date", "currency", "amount", "exchange_rate", "exchange_fee", "category", "subcategory", "budgetary", "subbudgetary", "corresponding_transaction", "transaction_id", "payee", "exb", "split", "note", "pn", "pc", "ma", "ar", "au", "re", "fi", "vo", "ba", "mo"]
            transaction_df = pd.DataFrame(columns=transaction_cols)
            party_cols = ["id", "name"]
            party_df = pd.DataFrame(columns=party_cols)
            category_cols = ["id", "name"]
            category_df = pd.DataFrame(columns=category_cols)
            subcategory_cols = ["id", "parent", "scatg_id", "name"]
            subcategory_df = pd.DataFrame(columns=subcategory_cols)
            budgetary_cols = ["id", "name"]
            budgetary_df = pd.DataFrame(columns=budgetary_cols)
            subbudgetary_cols = ["id", "parent", "sbudg_id", "name"]
            subbudgetary_df = pd.DataFrame(columns=subbudgetary_cols)
            currency_link_cols = ["id", "from", "to", "value"]
            currency_link_df = pd.DataFrame(columns=currency_link_cols)

            # parse file
            with open(filepath, 'r') as gsb_file:
                # retreive root of the xml file
                tree = et.parse(gsb_file)
                tree_root = tree.getroot()

                # compute number of nodes and initialize node parsed counter
                nb_child = len(tree_root.getchildren())
                current_child = 0

                lg.info(f"Start parsing {filepath}, that has {nb_child} nodes.")

                for child in tree_root:
                    current_child +=1
                    lg.debug(f"Parsing node {current_child}/{nb_child}.")

                    # check children tag and parse its attributes into relevant df accordingly
                    if child.tag == "Currency":
                        # read data from child node
                        id_number = child.attrib.get("Nb")
                        name = child.attrib.get("Na")
                        symbol = child.attrib.get("Co")
                        trigram = child.attrib.get("Ico")

                        # append data as a new line of the df
                        currency_df = currency_df.append(
                            pd.Series([id_number, name, symbol, trigram], index=currency_cols),
                            ignore_index=True
                        )

                    elif child.tag == "Account":
                        "id", "name", "currency", "initial_balance"
                        id_number = child.attrib.get("Number")
                        name = child.attrib.get("Name")
                        currency = child.attrib.get("Currency")
                        initial_balance = float(child.attrib.get("Initial_balance"))

                        account_df = account_df.append(
                            pd.Series([id_number, name, currency, initial_balance], index=account_cols),
                            ignore_index=True
                        )

                    elif child.tag == "Transaction":
                        id_number = child.attrib.get("Nb")
                        account = child.attrib.get("Ac")
                        # the "Dt" attribute is formatted "12/31/1999" (gasp!) alike
                        _date_string = child.attrib.get("Dt")
                        # if no date, assume it's tomorrow
                        if _date_string == '':
                            date = datetime.date.today() + datetime.timedelta(days=1)
                        else:
                            date = datetime.datetime.strptime(_date_string, "%m/%d/%Y")
                        _value_date_string = child.attrib.get("Dv")
                        # if no date, assume it's tomorrow
                        if _value_date_string == '':
                            value_date = datetime.date.today() + datetime.timedelta(days=1)
                        else:
                            value_date = datetime.datetime.strptime(_value_date_string, "%m/%d/%Y")
                        currency = child.attrib.get("Cu")
                        amount = float(child.attrib.get("Am"))
                        exchange_rate = float(child.attrib.get("Exr"))
                        exchange_fee = float(child.attrib.get("Exf"))
                        category = child.attrib.get("Ca")
                        subcategory = child.attrib.get("Sca")
                        budgetary = child.attrib.get("Bu")
                        subbudgetary = child.attrib.get("Sbu")
                        corresponding_transaction = child.attrib.get("Trt")
                        transaction_id = child.attrib.get("Id")
                        payee = child.attrib.get("Pa")
                        exb = child.attrib.get("Exb")
                        split = bool(int(child.attrib.get("Br")))
                        note = child.attrib.get("No")
                        pn = child.attrib.get("Pn")
                        pc = child.attrib.get("Pc")
                        ma = child.attrib.get("Ma")
                        ar = child.attrib.get("Ar")
                        au = child.attrib.get("Au")
                        re = child.attrib.get("Re")
                        fi = child.attrib.get("Fi")
                        vo = child.attrib.get("Vo")
                        ba = child.attrib.get("Ba")
                        mo = child.attrib.get("Mo")

                        transaction_df = transaction_df.append(
                            pd.Series([id_number, account, date, value_date, currency, amount, exchange_rate, exchange_fee, category, subcategory, budgetary, subbudgetary, corresponding_transaction, transaction_id, payee, exb, split, note, pn, pc, ma, ar, au, re, fi, vo, ba, mo], index=transaction_cols),
                            ignore_index=True
                        )

                    elif child.tag == "Party":
                        id_number = child.attrib.get("Nb")
                        name = child.attrib.get("Na")

                        party_df = party_df.append(
                            pd.Series([id_number, name], index=party_cols),
                            ignore_index=True
                        )

                    elif child.tag == "Category":
                        id_number = child.attrib.get("Nb")
                        name = child.attrib.get("Na")

                        category_df = category_df.append(
                            pd.Series([id_number, name], index=category_cols),
                            ignore_index=True
                        )

                    elif child.tag == "Sub_category":
                        id_number = child.attrib.get("Nbc") + "_" + child.attrib.get("Nb")
                        parent = child.attrib.get("Nbc")
                        scatg_id = child.attrib.get("Nb")
                        name = child.attrib.get("Na")

                        subcategory_df = subcategory_df.append(
                            pd.Series([id_number, parent, scatg_id, name], index=subcategory_cols),
                            ignore_index=True
                        )

                    elif child.tag == "Budgetary":
                        id_number = child.attrib.get("Nb")
                        name = child.attrib.get("Na")

                        budgetary_df = budgetary_df.append(
                            pd.Series([id_number, name], index=budgetary_cols),
                            ignore_index=True
                        )

                    elif child.tag == "Sub_budgetary":
                        id_number = child.attrib.get("Nbb") + "_" + child.attrib.get("Nb")
                        parent = child.attrib.get("Nbb")
                        sbudg_id = child.attrib.get("Nb")
                        name = child.attrib.get("Na")

                        subbudgetary_df = subbudgetary_df.append(
                            pd.Series([id_number, parent, sbudg_id, name], index=subbudgetary_cols),
                            ignore_index=True
                        )

                    elif child.tag == "Currency_link":
                        id_number = child.attrib.get("Nb")
                        from_curr = child.attrib.get("Cu1")
                        to_curr = child.attrib.get("Cu2")
                        value = float(child.attrib.get("Ex"))

                        currency_link_df = currency_link_df.append(
                            pd.Series([id_number, from_curr, to_curr, value], index=currency_link_cols),
                            ignore_index=True
                        )

                    else:
                        continue

                lg.info("Parsing done.")

    # create a global DataFrame

    # export DataFrame

if __name__ == '__main__':
    main()
