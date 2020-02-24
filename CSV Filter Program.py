import pandas as pd
from configparser import ConfigParser
import os
import shutil
import logging
from datetime import datetime
import sys


def column_filter():

    PATH = os.environ['FILTER_PATH']
    cfg = ConfigParser()
    cfg.optionxform = str
    now = datetime.now()
    path_config_file = os.path.join(os.environ['FILTER_PATH'], 'CONF.cfg')
    log_file = 'Logger.log'
    logging.basicConfig(filename=log_file, level=logging.DEBUG)
    if os.path.exists(os.path.join(os.environ['FILTER_PATH'], 'CONF.cfg')) and cfg.read(path_config_file):
        cfg.read(path_config_file)
    else:
        logging.debug('\n************ERROR at ' +now.strftime("%Y-%m-%d %H:%M:%S")+ '***********' + '\nCurrent Working Directory is set to '
                      + PATH +'\nCONF file not present in Current Working Directory ' + PATH +
                      '\nPlease make sure your System Variable is set to Current Working Directory or make sure your CONF '
                      'file is present in Current Working Dorectory')
        sys.exit()
    try:
        source_file = cfg.get('File Name', 'Source File')
        if os.path.exists(os.path.join(os.environ['FILTER_PATH'], source_file)):
            path_source_csv = os.path.join(os.environ['FILTER_PATH'], source_file)
        else:
            logging.debug('\n************ERROR at ' + now.strftime(
            "%Y-%m-%d %H:%M:%S") + 'Source CSV File Not Found in Current Working Directory. Make sure source csv is in '
                          'current working directory and add its name to CONF.cfg')
            sys.exit()

        colname1 = cfg.get('Column Details', 'Column1')
        colname2 = cfg.get('Column Details', 'Column2')
    except Exception as e:
        logging.debug('\n************ERROR at ' + now.strftime(
            "%Y-%m-%d %H:%M:%S") + '***********' + "\nPlease make Sure CONF.cfg File is in below format:"+"\n"
                "[File Name]"+"\nSource File= Your_SourceCSV_File.csv"+"\n[Column Details]"
                 + "\nColumn1 = Column No 1 to filter"+"\nColumn2 = Column No 1 to filter")

    try:
        data = pd.read_csv(path_source_csv)
        data_wo_nan = data.dropna()
        total_unique_values_col1 = data[colname1].unique()
        total_unique_values_col2 = data[colname2].unique()
        path_output = os.path.join(PATH, "output_folder")
        if os.path.exists(os.path.join(os.environ['FILTER_PATH'], "output_folder")):
            shutil.rmtree(path_output)
            logging.debug('\n************ERROR at ' + now.strftime(
                "%Y-%m-%d %H:%M:%S") + '***********' + 'output_older is already present in '+PATH+" \nPlease delete "
                                     "output Folder."
                                    "Please make sure all the CSV files are in closed and not open in any application")

            # sys.exit()
        os.mkdir(path_output)
        for val1 in total_unique_values_col1:
            path_output_col1 = os.path.join(path_output, str(val1))
            os.mkdir(path_output_col1)
            df_final = data_wo_nan.loc[data_wo_nan[colname1] == val1]
            for val2 in total_unique_values_col2:
                df_final_csv = df_final.loc[df_final[colname2] == val2]
                if not df_final_csv.empty:
                    path_final_csv = os.path.join(path_output_col1, str(val1) + "_" + str(val2) + ".csv")
                    df_final_csv.to_csv(path_final_csv, header=True)

    except Exception as e:
        logging.debug(e)
        logging.exception('\n************ERROR at ' + now.strftime(
            "%Y-%m-%d %H:%M:%S") + '***********' + 'Error in Column Names in CONF.cfg file. Make sure column name is'
                                                   ' present in the columns of source csv file.')


if __name__ == '__main__':
    column_filter()
