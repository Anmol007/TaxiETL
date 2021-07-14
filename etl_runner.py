import os

from TaxiETL import TaxiETL

ldir = os.path.dirname(__file__)


def run():
	"""
	Runs complete ETL
	"""
	ea = TaxiETL()
	print("EXTRACTING DATA..")
	ea.extract()
	print("TRANSFORMING DATA..")
	ea.transform()
	print("LOADING DATA..")
	ea.load()


if __name__ == "__main__":
	run()