from math import isnan

import PATHS
import pandas as pd
from datetime import datetime
from functools import partial


# TODO: Add data validation test
# TODO: Add date validation in excel for "date acquired" column in ce
# TODO: Add validation for 'Contacts' sheet in ce
# TODO: Service frequency N/A
# TODO: Delete date formatting?

def adjust_excel_column_widths(df: pd.DataFrame,
                               writer: pd.ExcelWriter,
                               sheet_name: str):
    """Adjust column widths for writer and df in sheet sheet_name."""
    for column in df:
        column_length = max(df[column].astype(str).map(len).max(),
                            len(str(column))) + 1
        col_idx = df.columns.get_loc(column)
        writer.sheets[sheet_name].set_column(col_idx, col_idx, column_length)


class QueriesLib:

    def __init__(self) -> None:
        # Load current equipment info:
        self.ce = pd.read_excel(PATHS.EQUIPMENT_INFORMATION,
                                sheet_name="Current Equipment"
                                )
        self.ce = self.ce[self.ce.Category != 'Archived']
        self.ce_to_timedelta()  # Strings to timedeltas.

        # Load events log:
        self.el = pd.read_excel(PATHS.EQUIPMENT_INFORMATION,
                                sheet_name="Event Log")

        # Extract service log (services and installations done or due but not
        # the scheduled ones):
        self.sl = self.el[(self.el['Event Type'] == 'Installation')
                          | (self.el['Event Type'] == 'Service')][
            ['CRGH ID', 'Date', 'Event Type']]
        self.sl = self.sl[~self.sl['Date'].isnull()]

        # Extract scheduled services log:
        self.ssl = self.el[
            (self.el['Event Type'] == 'Installation')
            | (self.el['Event Type'] == 'Service')]
        self.ssl = self.ssl[self.ssl['Date'].isnull()][
            ['CRGH ID', 'Date Scheduled']]

    def ce_to_timedelta(self) -> None:
        """Swaps 'Service frequency' strings in self.ce for corresponding
        timedeltas."""
        # TODO: Remove '3 monthly' and '6 monthly' from sfd once db is updated.
        sfd = {"Yearly": pd.Timedelta(days=365),
               "Biyearly": pd.Timedelta(days=182),
               "6 monthly": pd.Timedelta(days=182),
               "Quarterly": pd.Timedelta(days=91),
               "3 monthly": pd.Timedelta(days=91),
               "N/A": pd.Timedelta(days=int(1e5))}
        self.ce["Service frequency"].replace(sfd, inplace=True)

    def last_services(self) -> None:
        """Returns the last time each machine was serviced."""
        return self.sl[['CRGH ID', 'Date']].groupby('CRGH ID').max()

    def next_services(self) -> None:
        """Returns the next time each machine is scheduled to be serviced."""
        return self.ssl.groupby('CRGH ID').min().rename(
            columns={'Date Scheduled': 'Next service/installation scheduled'})

    def upcoming_services(self,
                          period_length: pd.Timedelta = pd.Timedelta(days=365)
                          ) -> pd.DataFrame:
        """Returns services due by today + period_length in ascending order."""
        dates, ids = [], []
        ls = self.last_services().join(
            self.ce[['CRGH ID', 'Service frequency']].set_index(['CRGH ID']))
        for _, row in ls.iterrows():
            next_service = row['Date'] + row['Service frequency']
            while next_service <= (pd.to_datetime('today') + period_length):
                dates.append(next_service)
                ids.append(row.name)
                next_service += row['Service frequency']
        us = pd.DataFrame({'CRGH ID': ids, 'Service due date': dates})

        # Add next scheduled services column
        us = us.set_index('CRGH ID').join(self.next_services(),
                                          how='outer')
        us['Min date'] = us.groupby('CRGH ID')['Service due date'].min()
        mask = ((us['Min date'] != us['Service due date']) & (
            ~us['Service due date'].isnull()))
        us['Next service/installation scheduled'][mask] = pd.NaT
        us = us.drop(columns=['Min date'])

        # Sort by next date:
        key_col = []
        dates1, dates2 = us['Service due date'], us[
            'Next service/installation scheduled']
        for d1, d2 in zip(dates1, dates2):
            if (not pd.isnull(d1)) and (not pd.isnull(d2)):
                key_col.append(min(d1, d2))
            elif not pd.isnull(d1):
                key_col.append(d1)
            elif not pd.isnull(d2):
                key_col.append(d2)
            else:
                raise AssertionError
        us['key'] = key_col
        us = us.sort_values(by='key').drop(columns=['key'])

        return us.reset_index()

    def services_in_range(self,  start_date: datetime, end_date: datetime):
        """Returns all services and installations, done or due, in range
        [start_date, end_date]."""
        # Extract services already occurred.
        ps_in_range = pd.DataFrame
        days_after_start_date = pd.to_datetime('today') - start_date
        # If start_date is earlier than today locate past services in range:
        if days_after_start_date >= pd.Timedelta(0):
            ps_in_range = self.sl.loc[(self.sl['Date'] >= start_date)
                                      & (self.sl['Date'] <= end_date)][
                ['CRGH ID', 'Date', 'Event Type']]
            # Format dates:
            ps_in_range['Date'] = ps_in_range['Date'].dt.strftime('%B, %Y')

        # Define dictionary recording the type of each event:
        type_dict = {(row['CRGH ID'], row['Date']): row['Event Type'] for
                     _, row in ps_in_range.iterrows()}  #
        ps_in_range.drop(
            columns=['Event Type'])  # We don't need these anymore.

        # Extract services upcoming.
        us_in_range = pd.DataFrame
        days_until_end_date = end_date - pd.to_datetime('today')
        # If end_date is later than today locate upcoming services in range:
        if days_until_end_date >= pd.Timedelta(0):
            us = self.upcoming_services(days_until_end_date).rename(
                columns={'Service due date': 'Date'})
            us_in_range = us.loc[(us['Date'] >= start_date)
                                 & (us['Date'] <= end_date)]

        # Update type_dict with new types.
        type_dict |= {(row['CRGH ID'], row['Date'].strftime('%B, %Y')): 'Due'
        if row['Date'] > pd.to_datetime('today') else 'Overdue'
                      for _, row in us_in_range.iterrows()}

        # Format dates:
        if days_until_end_date >= pd.Timedelta(0):
            us_in_range['Date'] = us_in_range['Date'].dt.strftime('%B, %Y')


        # Gather all services in range:
        s_in_range = pd.concat([ps_in_range, us_in_range])

        # Create and insert counts column for the long-to-wide pivot in
        # https://stackoverflow.com/questions/22798934/pandas-long-to-wide
        # -reshape-by-two-variables:
        counts_per_id = {i: 0 for i in s_in_range['CRGH ID']}
        counts = []
        for i in s_in_range['CRGH ID']:
            counts_per_id[i] += 1
            counts.append(counts_per_id[i])
        s_in_range.insert(len(list(s_in_range)), 'counts', counts)

        # Pivot:
        s_in_range = s_in_range.pivot(index='CRGH ID',
                                      columns='counts',
                                      values='Date')
        s_in_range.columns.name = None  # Clear spurious name.

        # Add Category column:
        s_in_range = s_in_range.join(
            self.ce[['CRGH ID', 'Category']].set_index(
                'CRGH ID')).reset_index().set_index(
            'Category').sort_index().reset_index()

        return s_in_range, type_dict

    def run_all(self, period: pd.Timedelta = pd.Timedelta(days=365),
                start_date: datetime = pd.to_datetime('01/01/2022'),
                end_date: datetime = pd.to_datetime('01/01/2023')):
        # Set up writer:
        writer = pd.ExcelWriter(PATHS.QUERIES, datetime_format='DD-MM-YYYY')

        # Upcoming services:
        us = self.upcoming_services(period)
        # # Datetime formatting:
        us['Service due date'] = us['Service due date'].dt.strftime('%B, %Y')
        # To excel:
        us.to_excel(writer, sheet_name='Upcoming Services', index=False)
        adjust_excel_column_widths(us, writer, 'Upcoming Services')

        # Services/installations in [start_date, end_date].
        sir, type_dict = self.services_in_range(start_date, end_date)
        # Add color highlights:
        sir_h = sir.style.apply(partial(highlight_sir, dict=type_dict), axis=1)
        # Format dates:
        # To excel:
        sir_h.to_excel(writer, sheet_name='Services in range', index=False)
        adjust_excel_column_widths(sir, writer, 'Services in range')

        writer.save()

def highlight_sir(row: pd.DataFrame, dict: dict = None):
    type_to_color = {'Due': 'background-color: yellow',
                     'Service': 'background-color: green',
                     'Overdue': 'background-color: red',
                     'Installation': 'background-color: blue'}
    color_dict = {key: type_to_color[val] for key, val in dict.items()}
    return ['', ''] + [color_dict[(row['CRGH ID'], v)] if isinstance(v, str)
                       else ''for v in row[2:]]
