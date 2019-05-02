from db_adapter.base import get_Pool

from db_adapter.curw_fcst.source import add_sources, get_source_id, get_source_by_id, delete_source, delete_source_by_id

USERNAME = "root"
PASSWORD = "password"
HOST = "127.0.0.1"
PORT = 3306
DATABASE = "test_schema"

sources = [
        {
                'model'     : 'FLO2D_150',
                'version'   : '',
                'parameters': {
                        "CHANNEL_CELL_MAP"               : {
                                "429"  : "Wellawatta Canal-St Peters College", "568": "Mutwal Outfall",
                                "668"  : "Dehiwala Canal", "1046": "Thummodara", "1145": "Swarna Rd-Wellawatta",
                                "1417" : "Babapulle", "1546": "Ingurukade Jn", "1675": "Nagalagam Street",
                                "1762" : "Torrinton", "1919": "Dematagoda Canal-Orugodawatta",
                                "1931" : "Nagalagam Street River", "2005": "OUSL-Narahenpita Rd",
                                "2281" : "LesliRanagala Mw", "2392": "Kirimandala Mw",
                                "2515" : "OUSL-Nawala Kirulapana Canal", "2561": "Kittampahuwa", "2812": "Near SLLRDC",
                                "2841" : "Kalupalama", "3108": "Yakbedda", "3131": "Kittampahuwa River",
                                "3576" : "Vivekarama Mw", "3730": "Wellampitiya", "4045": "Madinnagoda",
                                "4228" : "Harwad Band", "4566": "Kotte North Canal", "4748": "Kotiyagoda",
                                "5281" : "Kotte road bridge", "5482": "Koratuwa Rd", "6190": "Weliwala Pond",
                                "6336" : "Parlimant Lake Side", "6733": "Salalihini-River", "6862": "JanakalaKendraya",
                                "6901" : "Kelani Mulla Outfall", "7409": "Old Awissawella Rd",
                                "7572" : "Talatel Culvert", "7744": "Wennawatta", "10195": "Ambatale Outfull1",
                                "10198": "Ambatale River", "11045": "Amaragoda", "11796": "Malabe", "24799": "Hanwella",
                                "40608": "Glencourse"
                                }, "FLOOD_PLAIN_CELL_MAP": { }
                        }
                },
        {
                'model'     : 'OBS_WATER_LEVEL',
                'version'   : '',
                'parameters': {
                        "CHANNEL_CELL_MAP"               : {
                                "594" : "Wellawatta", "1547": "Ingurukade", "3255": "Yakbedda", "3730": "Wellampitiya",
                                "7033": "Janakala Kendraya"
                                }, "FLOOD_PLAIN_CELL_MAP": { }
                        }
                },
        {
                'model'     : 'wrfA',
                'version'   : 'v3',
                'parameters': { }
                },
        {
                'model'     : 'wrfC',
                'version'   : 'v3',
                'parameters': { }
                },
        {
                'model'     : 'wrfE',
                'version'   : 'v3',
                'parameters': { }
                },
        {
                'model'     : 'wrfSE',
                'version'   : 'v3',
                'parameters': { }
                }
        ]

pool = get_Pool(host=HOST, port=PORT, user=USERNAME, password=PASSWORD, db=DATABASE)

print("########### Add Sources #################################")
print(add_sources(sources=sources, pool=pool))


print("########### Get Sources by id ###########################")
print("Id 24:", get_source_by_id(pool=pool, id_="24"))


print("########## Retrieve source id ###########################")
print("'model': 'OBS_WATER_LEVEL', 'version': ''", get_source_id(pool=pool, model="OBS_WATER_LEVEL", version=""))


print("######### Delete source by id ###########################")
print("Id 24 deleted status: ", delete_source_by_id(pool=pool, id_=24))

print("######### Delete source with given model, version #######")
print("model': 'wrfSE', 'version': 'v3' delete status :",
        delete_source(pool=pool, model="wrfSE", version="v3"))

pool.destroy()
exit()