
import pymongo, traceback
from SETTINGS import MONGO_URL


def get_covid_19_database():
    # conn = pymongo.MongoClient(MONGO_URL)
    conn=pymongo.MongoClient(MONGO_URL)
    return DBHandler(connection=conn, db_name="covid_19")

# def get_db_2():
# 	# conn = pymongo.MongoClient(MONGO_URL)
# 	conn=pymongo.MongoClient(MONGO_URL_2)
# 	return conn[MONGO_DB]


class DBHandler:

    db_handler = None

    def __init__(self, connection, db_name):

        self.db_handler = connection[db_name]


    def get_raw_handler(self):
        return self.db_handler

    def aggregator_query(self, collection, pipeline):
        """
        :param collection: master collection
        :param pipeline: pipeline with join
        :return: List (record)
        """

        rows = []
        try:
            cursor = self.db_handler[collection].aggregate(pipeline, allowDiskUse=True)
            # print("cursor aggregator_query", cursor)
            for item in cursor:
                if "_id" in item:
                    item['_id'] = str(item['_id'])
                rows.append(item)
            # del rows["staymx_reservation_customer"]
            # print("row in aggregator query, ", rows)
            return rows
        except Exception as e:
            print(str(e))
            print(traceback.format_exc())
            return []

    def aggregator_query_return_cursor(self, collection, pipeline):
        """
        :param collection: master collection
        :param pipeline: pipeline with join
        :return: cursor (Object)
        """
        rows = []
        try:
            cursor = self.db_handler[collection].aggregate(pipeline, allowDiskUse=True)
            return cursor
        except Exception as e:
            print(str(e))
            print(traceback.format_exc())
            return []

    def aggregator_query_count(self, collection, pipeline):
        """
        :param collection:
        :param pipeline:
        :return: Int (number of row count)
        """
        try:
            cursor_list = list(self.db_handler[collection].aggregate(pipeline, allowDiskUse=True))
            if len(cursor_list) < 1:
                return 0

            return cursor_list[0]["count"]
        except Exception as e:
            print(str(e))
            print(traceback.format_exc())
            return 0

    def get_rows_cursor(self, collection, query, fields={}, sort={}, skip=0, limit=10000):
        """
        :param collection:
        :param query:
        :param fields:
        :param sort:
        :param skip:
        :param limit:
        :return: cursor (object)
        """
        if sort == {}:
            if fields == {}:
                cursor = self.db_handler[collection].find(query).skip(skip).limit(limit)
            else:
                cursor = self.db_handler[collection].find(query, fields).skip(skip).limit(limit)
        else:
            if fields == {}:
                cursor = self.db_handler[collection].find(query).sort(sort["key"], sort["direction"]).skip(skip).limit(limit)
            else:
                cursor = self.db_handler[collection].find(query, fields).sort(sort["key"], sort["direction"]).skip(skip).limit(limit)
        return cursor

    def get_rows(self, collection, query, fields={}, sort={}, skip=0, limit=30000):
        """
        :param collection:
        :param query:
        :param fields:
        :param sort:
        :param skip:
        :param limit:
        :return: list
        """

        rows = []
        if sort == {}:
            if fields == {}:
                cursor = self.db_handler[collection].find(query).skip(skip).limit(limit)
            else:
                cursor = self.db_handler[collection].find(query, fields).skip(skip).limit(limit)
        else:
            if fields == {}:
                cursor = self.db_handler[collection].find(query).sort(sort["key"], sort["direction"]).skip(skip).limit(limit)
            else:
                cursor = self.db_handler[collection].find(query, fields).sort(sort["key"], sort["direction"]).skip(skip).limit(limit)

        # print("cursor get data rows", cursor)
        for item in cursor:
            if "_id" in item:
                item['_id'] = str(item['_id'])
            rows.append(item)
        return rows

    # def get_rows_2(collection, query, fields={}, sort={}, skip=0, limit=5000):
    # 	db=get_db_2()
    # 	rows=[]
    # 	if sort=={}:
    # 		if fields=={}:
    # 			cursor=db[collection].find(query).skip(skip).limit(limit)
    # 		else:
    # 			cursor = db[collection].find(query, fields).skip(skip).limit(limit)
    # 	else:
    # 		if fields=={}:
    # 			cursor=db[collection].find(query).sort(sort["key"],sort["direction"]).skip(skip).limit(limit)
    # 		else:
    # 			cursor = db[collection].find(query, fields).sort(sort["key"], sort["direction"]).skip(skip).limit(limit)

    # 	for item in cursor:
    # 		if "_id" in item:
    # 			item['_id'] = str(item['_id'])
    # 		rows.append(item)
    # 	return rows

    def get_row(self, collection, query, fields={}, sort={}):
        """

        :param collection:
        :param query:
        :param fields:
        :param sort:
        :return: dict
        """


        if fields == {}:
            row = self.db_handler[collection].find_one(query)
        else:
            row = self.db_handler[collection].find_one(query, fields)
        if row is not None:
            if "_id" in row:
                row['_id'] = str(row['_id'])
        return row

    def get_count(self, collection, query):

        return self.db_handler[collection].find(query).count()

    def insert(self, collection, data):

        insert_id = self.db_handler[collection].insert(data, check_keys=False)
        return str(insert_id)

    def insert_many(self, collection, data):

        returned_ids = self.db_handler[collection].insert_many(data)
        # print returned_ids.insertedIds
        inserted_ids = [str(insert_id) for insert_id in returned_ids.inserted_ids]
        return inserted_ids

    def update(self, collection, update_data, query):

        return self.db_handler[collection].update(query, update_data, multi=True)

    def upsert(self, collection, update_data, query):

        return self.db_handler[collection].update(query, update_data, multi=True, upsert=True)

    def remove(self, collection, query):

        return self.db_handler[collection].remove(query)


