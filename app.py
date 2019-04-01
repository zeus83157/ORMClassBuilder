import pyodbc
import os

Server = "DESKTOP-OON9QKV"
user = "8021"
password = "8021"
database = "ResearchData"
engine = "mssql+pyodbc://8021:8021@DESKTOP-OON9QKV/ResearchData?driver=SQL+Server+Native+Client+11.0"

def SingleFrom(tName, data, pkDic, cnxn, datatypeDic, fkDic):
	
	if tName != "sysdiagrams":

		pkColumn = pkDic[tName]

		data = data + "\n\n#{0} Table".format(tName)
		tmp = "{0}_metadata".format(tName.lower())
		data = data + "\n{0} = MetaData()".format(tmp)
		data = data + "\n\n{0} = Table('{1}', {2}, ".format(tName.lower(), tName, tmp)

		cursor = cnxn.cursor()
		cursor.execute("SELECT * FROM Information_Schema.COLUMNS WHERE TABLE_NAME = '" + tName + "'")
		rows2 = cursor.fetchall()
		

		for row2 in rows2:
			data = data + "\n			Column('{0}',".format(row2.COLUMN_NAME)
			
			data = data + " " + datatypeDic[row2.DATA_TYPE]
			if row2.DATA_TYPE == "decimal":
				data = data + "({0}, {1})".format(row2.NUMERIC_PRECISION_RADIX, row2.NUMERIC_SCALE)

			if row2.DATA_TYPE == "nvarchar":
				data = data + "({0})".format(row2.CHARACTER_MAXIMUM_LENGTH)

			data = data + ","

			if row2.COLUMN_NAME == pkColumn:
				data = data + " primary_key = True,"

			if tName + row2.COLUMN_NAME in list(fkDic.keys()):
				fkTmp = fkDic[tName + row2.COLUMN_NAME]
				data = data + " ForeignKey({0}.{1}),".format(fkTmp[0],fkTmp[1])

			if row2.IS_NULLABLE == "NO":
				data = data + " nullable = False"





			data = data + "),"
		data = data + "\n			)"


		data = data + "\n\nclass {0}(object):".format(tName)

		data = data + "\n	def __init__(self,"
		for row2 in rows2:
			data = data + " {0},".format(row2.COLUMN_NAME)
		data = data + "):"


		for row2 in rows2:
			data = data + "\n		self.{0} = {0}".format(row2.COLUMN_NAME)

		data = data + "\n\nmapper({0}, {1})".format(tName, tName.lower())

		data = data + "\n\n\n\n"
	return data

def BuildORMClass():

	datatypeDic = {
	"bit":"BIT",
	"decimal":"DECIMAL",
	"int":"INTEGER",
	"nvarchar":"NVARCHAR",
	"uniqueidentifier":"UNIQUEIDENTIFIER",
	}


	data = "from sqlalchemy import create_engine"\
	+ "\nfrom sqlalchemy.ext.declarative import declarative_base"\
	+ "\nfrom sqlalchemy.dialects.mssql import \\"\
	+ "\n	BIGINT, BINARY, BIT, CHAR, DATE, DATETIME, DATETIME2, \\"\
	+ "\n	DATETIMEOFFSET, DECIMAL, FLOAT, IMAGE, INTEGER, MONEY, \\"\
	+ "\n	NCHAR, NTEXT, NUMERIC, NVARCHAR, REAL, SMALLDATETIME, \\"\
	+ "\n	SMALLINT, SMALLMONEY, SQL_VARIANT, TEXT, TIME, \\"\
	+ "\n	TIMESTAMP, TINYINT, UNIQUEIDENTIFIER, VARBINARY, VARCHAR"\
	+ "\nfrom sqlalchemy import Table, MetaData, Column, Integer, String, ForeignKey, Sequence"\
	+ "\nfrom sqlalchemy.orm import mapper"\
	+ "\nfrom sqlalchemy.orm import sessionmaker"

	data = data + "\n\nengine = create_engine('{0}')".format(engine)
	data = data + "\n\nBase = declarative_base()"




	cnxn = pyodbc.connect("DRIVER={SQL Server}; SERVER=" + Server + "; DATABASE=" + database + "; UID=" + user + "; PWD=" + password)
	cursor = cnxn.cursor()



	pkDic = {}
	cursor.execute("SELECT column_name, TABLE_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE OBJECTPROPERTY(OBJECT_ID(constraint_name), 'IsPrimaryKey') = 1")
	rows3 = cursor.fetchall()
	for row3 in rows3:
		pkDic[row3.TABLE_NAME] = row3.column_name


	fkDic = {}
	cursor.execute("SELECT f.name AS foreign_key_name ,OBJECT_NAME(f.parent_object_id) \
		AS table_name ,COL_NAME(fc.parent_object_id, fc.parent_column_id) \
		AS constraint_column_name ,OBJECT_NAME (f.referenced_object_id) \
		AS referenced_object ,COL_NAME(fc.referenced_object_id, fc.referenced_column_id) \
		AS referenced_column_name ,is_disabled ,delete_referential_action_desc ,update_referential_action_desc \
		FROM sys.foreign_keys AS f INNER JOIN sys.foreign_key_columns AS fc ON f.object_id = fc.constraint_object_id ;")

	rows4 = cursor.fetchall()
	for row4 in rows4:
		fkDic[row4.table_name + row4.constraint_column_name] = (row4.referenced_object, row4.referenced_column_name)



	cursor.execute("SELECT distinct(TABLE_NAME) FROM Information_Schema.COLUMNS where TABLE_NAME not in\
					(SELECT OBJECT_NAME(f.parent_object_id) AS table_name \
					FROM sys.foreign_keys AS f INNER \
					JOIN sys.foreign_key_columns AS fc ON f.object_id = fc.constraint_object_id)")
	rows = cursor.fetchall()
	tmprows = rows
	for row in rows:
		data = SingleFrom(row.TABLE_NAME, data, pkDic, cnxn, datatypeDic, fkDic)




	cursor.execute("SELECT OBJECT_NAME(f.parent_object_id) AS table_name, OBJECT_NAME (f.referenced_object_id) AS referenced_object\
					FROM sys.foreign_keys AS f INNER JOIN sys.foreign_key_columns AS fc ON f.object_id = fc.constraint_object_id\
					WHERE OBJECT_NAME(f.parent_object_id) in\
					(SELECT distinct(TABLE_NAME) FROM Information_Schema.COLUMNS where TABLE_NAME in\
					(SELECT OBJECT_NAME(f.parent_object_id) AS table_name \
					FROM sys.foreign_keys AS f INNER \
					JOIN sys.foreign_key_columns AS fc ON f.object_id = fc.constraint_object_id))")
	rows = cursor.fetchall()

	tmpRTDic = {}
	for row in rows:
		if row.table_name in list(tmpRTDic.keys()):
			tmpRTDic[row.table_name].append(row.referenced_object)
		else:
			tmpRTDic[row.table_name] = [row.referenced_object, ]
	print(tmpRTDic)
	# while len(tmprows) > 0:
	# 	for row in rows:
	# 		if row is in tmprows:
	# 			data = SingleFrom(row.TABLE_NAME, data, pkDic, cnxn, datatypeDic, fkDic)
	# 			tmprows.remove(row)



	cnxn.close()


	data = data + "\n\nBase.metadata.create_all(engine)"

	data = data + "\n\nSession = sessionmaker()"
	data = data + "\n\nSession.configure(bind=engine)"



	fp = open("DatabaseBuilder.py", "w", encoding = "utf-8")
	fp.write(data)
	fp.close()




if __name__ == '__main__':
	BuildORMClass()