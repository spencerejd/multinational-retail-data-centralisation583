import pandas as pd
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning



data_extractor = DataExtractor()
pdf_path = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
pdf_df = data_extractor.retrieve_pdf_data(pdf_path)
#print(pdf_df)
print(pdf_df.info())
data_cleaner = DataCleaning()
clean_pdf_df = data_cleaner.clean_card_data(pdf_df)
#print(clean_pdf_df)
#print(clean_pdf_df.info())
