from src.Data.generate_database import database_actualization, database_inicialization

mode = 2 # 1: Generate dataBase 2: update dataBase 3: Get game predictions
model = ""

if mode == 1:
    database_inicialization()
elif mode == 2:
    database_actualization()