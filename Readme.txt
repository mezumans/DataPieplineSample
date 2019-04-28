
#python version 3.6.1

DataTask
	Two modules: Producer and Consumer

To initate example, from DataPieplineSample dir :

To init producer:
python Producer rabbimq_host db_path year country
i.e:
python Producer localhost chinook.db 2009 USA

To init consumer:
python Consumer rabbimq_host
i.e:
python Consumer localhost




