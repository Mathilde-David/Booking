import numpy
import pandas as pd
import datetime
import unicodedata

def transform_data_unformatted(data):
    '''

    Transform Booking data

    data input :
        *booking_id
        *restaurant_id
        *restaurant_name
        *client_id
        *client_name
        *amount
        *guests(number of people for the given booking)
        *date
        *country

    data output :
        * restaurant_id
        * restaurant_name
        * country
        * month (in following format : YYYY_MM)
        * number_of_bookings
        * number_of_guests
        * amount
        * currency (use to test data consistency)
    '''

    # Replace character to simplify split in the for loop
    data["date"]= data["date"].str.replace("-", "/")
    data["amount"] = data["amount"].str.normalize("NFKD")

    # Regex to seperate
    amount_split = data["amount"].str.split("([^\d] ?)?([\d ]+,\d{2}|[\d,]+\.\d{2})( ?[^\d])?")
    amount_split_clean = [[s for s in element if s != '' and s != ' ' and s != None] for element in amount_split]
    amount_split_clean = pd.Series(sorted(element) for element in amount_split_clean)

    # Remove space in strings of amount_split_clean
    clean = []
    for index, items in amount_split_clean.iteritems():
        new_list = []
        for it in items :
            new_list.append(str(it).replace(" ", ""))
        clean.append(new_list)

    # Sort amount, that way the currency is after the value of the amount
    amount_split_sorted = pd.Series(sorted(element) for element in clean)

    amount_split_sorted = pd.DataFrame.from_dict(dict(zip(amount_split_sorted.index, amount_split_sorted.values)))

    # Create a currency column and an amount_value column cast as float
    data["currency"] = amount_split_sorted.loc[1,:]
    data["amount_value"] = amount_split_sorted.loc[0,:].str.replace(",", ".").astype(float)

    # Split date by /
    split_date = data["date"].str.split("/")
    split_date_dt = pd.DataFrame.from_dict(dict(zip(split_date.index, split_date.values)))
    # Format month column YYYY_MM
    data["month"] = split_date_dt.loc[2,:] + "_" + split_date_dt.loc[1,:]

    # Prepare the output dataframe
    output = data[["restaurant_id", "restaurant_name", "country", "month", "currency"]].drop_duplicates()

    # Calculate the number of booking group by restaurant and month
    bookings_agg = data.groupby(["restaurant_id", "month"]).size()
    # Calculate the sum of guests group by restaurant and month
    guest_agg = data.groupby(["restaurant_id", "month"])['guests'].agg('sum')
    # Calculate the sum of amount group by restaurant and month
    amount_agg = data.groupby(["restaurant_id", "month"])['amount_value'].agg('sum')

    for index, row in output.iterrows():

        output.loc[index,"number_of_bookings"] = bookings_agg.loc[row["restaurant_id"]].loc[row["month"]]
        output.loc[index,"number_of_guests"] = guest_agg.loc[row["restaurant_id"]].loc[row["month"]]

        # Format amount column
        if row["country"] == "United Kingdom":
            output.loc[index, "amount"] = row["currency"]+"{:.2f}".format(amount_agg.loc[row["restaurant_id"]].loc[row["month"]])
        else:
            string_amount = "{:.2f}".format(amount_agg.loc[row["restaurant_id"]].loc[row["month"]])+" "+row["currency"]
            output.loc[index, "amount"] = string_amount.replace(".", ",")

    # Cast int type for number_of_bookings and number_of_guests columns
    output["number_of_bookings"]= output["number_of_bookings"].astype(int)
    output["number_of_guests"] = output["number_of_guests"].astype(int)

    return output


def transform_data(data):
    """
        Call the transform_data_unformatted to transform the data
        Return the expected columns :
        * restaurant_id
        * restaurant_name
        * country
        * month (in following format : YYYY_MM)
        * number_of_bookings
        * number_of_guests
        * amount
    """
    print("START: Transform data", datetime.datetime.now())
    unformated_data = transform_data_unformatted(data)
    output = unformated_data[["restaurant_id", "restaurant_name", "country", "month", "number_of_bookings", "number_of_guests", "amount"]]
    print("END: Transform data", datetime.datetime.now())
    return output

