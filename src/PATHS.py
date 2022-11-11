from os.path import dirname, realpath

REPOSITORY = dirname(dirname(realpath(__file__)))
EQUIPMENT_INFORMATION = REPOSITORY + '/db/equipment_information.xlsx'
UPCOMING_SERVICES = REPOSITORY + '/queries/upcoming_services.xlsx'
SERVICES_IN_RANGE = REPOSITORY + '/queries/services_in_range.xlsx'
QUERIES = REPOSITORY + '/queries/queries.xlsx'