from os.path import dirname, realpath

REPOSITORY = dirname(realpath(__file__))
REPOSITORY_PARENT = dirname(dirname(realpath(__file__)))
EQUIPMENT_INFORMATION = REPOSITORY_PARENT + '/equipment_information.xlsx'
QUERIES = REPOSITORY_PARENT + '/queries.xlsx'
