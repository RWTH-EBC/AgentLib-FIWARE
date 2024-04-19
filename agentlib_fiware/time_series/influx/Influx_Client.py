import influxdb_client
import pandas as pd
import datetime
import pytz
import json
import os


class HttpsClient:
    """
    Https Client to query timeseries from InfluxDB
    """

    def __init__(self,
                 org: str = 'EBC',
                 token: str = 'tMg2XUqTI8CSPKgQ5y9K5WVlvUyTF62n7XY4UwPgLtx1pj9UzNv_GxskQvdpFmWn9OzZmY09RjBOY-c1uQ31Lg==',
                 url: str = 'https://ebc-tick-stack.westeurope.cloudapp.azure.com:8086',
                 timezone: str = 'CET'
                 ):
        """
        :param org: organisation in InfluxDB to connect to
        :param token: read-token in InfluxDB
        :param url: URL to connect to
        :param timezone: timezone to convert InfluxDB time to
        """
        client = influxdb_client.InfluxDBClient(
            timeout=120_000,  # in ms
            url=url,
            token=token,
            org=org,
        )

        self.query_api = client.query_api()
        self.org = org
        self.timezone = timezone

    @staticmethod
    def _load_config(filename):
        """
        Loads the configuration for a specific bucket.
        :param filename: filename of configuration
        :return:
        """
        root = os.getcwd()
        path = f'{root}\\symbol_lists\\{filename}'
        with open(
                path, 'r') as f:
            var_dict = json.load(f)
        return var_dict

    @staticmethod
    def _topic_string_builder(topic_list):
        """
        Builds a topic string in a format the query can use.
        :param topic_list:
        :return:
        """
        topic_str = str()
        for topic in topic_list[0:-1]:
            topic_str = ''.join([topic_str, f'r["topic"] == "{topic}" or '])

        # append last topic to topic_str
        topic_str = ''.join([topic_str, f'r["topic"] == "{topic_list[-1]}"'])

        return topic_str

    @staticmethod
    def _topic_string_builder_fiware(topic_list):
        """
        Builds a topic string in a format the query can use.
        :param topic_list:
        :return:
        """

        str1 = str()
        str2 = str()

        for idx, topic in enumerate(topic_list[0:-1]):
            str1 = ''.join([str1, f'r["service_path"] == "{topic[0]}" or '])
            str2 = ''.join([str2, f'r["_field"] == "{topic[1]}" or '])
            # fields_str = ''.join([fields_str, f'r["_field"] == "{topic[2]}" or '])

        # append last value to strings
        str1 = ''.join([str1, f'r["service_path"] == "{topic_list[-1][0]}"'])
        str2 = ''.join([str2, f'r["_field"] == "{topic_list[-1][1]}"'])
        # fields_str = ''.join([fields_str, f'r["field"] == "{topic_list[-1][2]}"'])

        topic_str = f'|> filter(fn: (r) => ({str1}) and ({str2}))'

        return topic_str

    def transform_dataframe_fiware(self, dfs):
        """
        Transforms dataframe from InfluxDB to dataframe in useful format.
        :param df: Dataframe queried from InfluxDB
        :param config:
        :return:
        """

        def _drop_result_and_table(_df):
            for col in ["result", "table"]:
                if col in _df.columns:
                    _df.drop(columns=[col], inplace=True)
            return _df

        def _add_field_values_to_combined_df(df, combined_df):
            for _field in df["_field"].unique():
                value_column = df.columns[-1]
                new_column_name = f'{value_column}/{_field}'
                combined_df.loc[:, new_column_name] = df.loc[df["_field"] == _field, value_column]
            return combined_df, df.loc[df["_field"] == _field, "_time"]

        # InfluxDB Client returns DataFrame with UTC-timestamps even if you requested it with timezone-aware timestamp
        combined_df = pd.DataFrame()
        if isinstance(dfs, list):
            for df in dfs:
                df = _drop_result_and_table(df)
                combined_df, time_index = _add_field_values_to_combined_df(df, combined_df)
        else:
            dfs = _drop_result_and_table(dfs)
            combined_df, time_index = _add_field_values_to_combined_df(dfs, combined_df)

        combined_df.set_index(pd.to_datetime(time_index, utc=True).dt.tz_convert(self.timezone), inplace=True)

        return combined_df

    def transform_dataframe(self, df, config):
        """
        Transforms dataframe from InfluxDB to dataframe in useful format.
        :param df: Dataframe queried from InfluxDB
        :param config:
        :return:
        """
        data_frame_sorted = pd.DataFrame()

        if config['params']['all_topics']:
            topic_list = df['topic'].unique()
        else:
            topic_list = config['topics']

        # InfluxDB Client returns DataFrame with UTC-timestamps even if you requested it with timezone-aware timestamp
        df["_time"] = pd.to_datetime(df["_time"], utc=True).dt.tz_convert(self.timezone)
        df.set_index("_time", inplace=True)

        grouped = df.groupby(["topic"], as_index=False)

        for topic in topic_list:
            temp_df = grouped.get_group(topic)['value'].to_frame()
            temp_df.rename(columns={'value': topic}, inplace=True)
            dti = temp_df.index.round(freq="100ms")
            temp_df.set_axis(dti, axis=0)
            if topic == topic_list[0]:
                data_frame_sorted = temp_df
            else:
                data_frame_sorted = data_frame_sorted.join(temp_df, how='outer')

        return data_frame_sorted

    def convert_datetime_str_format(self, time):
        time_str = pytz.timezone(self.timezone).localize(datetime.datetime.strptime(time, '%Y-%m-%d %H:%M:%S'))
        time_str = time_str.strftime("%Y-%m-%dT%H:%M:%S%z")
        time_str = time_str[:-2] + ':' + time_str[-2:]
        return time_str

    def query_builder(self, config, start, end):
        """
        Builds query for querying from InfluxDB.
        :param config:
        :param start:
        :param end:
        :return:
        """
        bucket = config['bucket']
        topic_list = config['topics']
        interval = config['params']['interval']
        all_topics = config['params']['all_topics']
        query_by_interval = config['params']['query_by_interval']

        start_str = self.convert_datetime_str_format(start)
        end_str = self.convert_datetime_str_format(end)

        if all_topics:
            topic_query = ''
        else:
            if config['structure'] == 'Fiware':
                topics_str = self._topic_string_builder_fiware(topic_list)
            else:
                topics_str = self._topic_string_builder(topic_list)
            if config['structure'] == 'Fiware':
                topic_query = topics_str
            else:
                topic_query = f'|> filter(fn: (r) => {topics_str})'

        if query_by_interval:
            window_query = f'|> aggregateWindow(every: {interval}, fn: last, createEmpty: true)'
        else:
            window_query = ''

        if config['structure'] == 'Fiware':
            pivot_str = '|> keep(columns: ["_time", "_value", "_field", "service_path"]) \
                         |> pivot(rowKey:["_time"], columnKey: ["service_path"], valueColumn: "_value")'

        else:
            pivot_str = '|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value") \
                         |> keep(columns: ["_time", "topic", "value"])'

        query_str = f'from(bucket: "{bucket}") \
                        |> range(start: {start_str}, stop: {end_str}) \
                        {topic_query} \
                        {window_query} \
                        {pivot_str}'
        print("Trying to get data with: ")
        print(query_str)
        return query_str

    def get_timeseries(self, config, start_time, end_time):
        """
        Function to load timeseries-data from InfluxDB.
        :param config_name:
        :param start_time:
        :param end_time:
        :return:
        """

        # config = self._load_config(config_name)

        query = self.query_builder(config=config, start=start_time, end=end_time)

        result_df = self.query_api.query_data_frame(org=self.org, query=query)
        print('Download completed!')
        print(result_df)

        if config['structure'] == 'Fiware':
            df_final = self.transform_dataframe_fiware(dfs=result_df)
        else:
            df_final = self.transform_dataframe(df=result_df, config=config)
        print('Transformation completed!')

        return df_final


if __name__ == "__main__":
    # Build HTTPS Client. For possible inputs see initialisation of class above.
    Client = HttpsClient()

    # Get the timeseries from InfluxDB. You need a config-json with bucket, params and topics.
    # For the format see example-configs.
    # Params:   interval: Interval the data is queried in.
    #           all_topics: if true, all topics in the chosen bucket are queried
    #           query_by_interval: if true, your timeseries data has the chosen resolution, if there
    #                              is no data at a time, it gets filled with "nan"
    #                              if false, your timeseries data only includes times where there is data

    # df_hil2 = Client.get_timeseries(config_name="config_read_optihorst_fiware.json",
    #                                 start_time='2023-08-17 07:30:00', end_time='2023-08-17 08:45:00'
    #                                )

    a=1
