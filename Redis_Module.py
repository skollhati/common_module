import redis
import BCloudModule.BModule_Singleton as Singleton
from BCloudModule.LogModule_Singleton import BCloudLogger_Singleton
import json
import pickle


class ConnectionObject():
    def __init__(self, db, password, timeout=3600, host='127.0.0.1', port=6379):
        self.redis_conn = redis.StrictRedis(host=host, port=port, db=db, password=password, charset="utf-8",
                                            decode_responses=True)
        # self.redis_conn=redis.Redis(connection_pool=self.redis_pool)

        if timeout > 0:
            self.timeout = timeout
        else:
            self.timeout = None

        # pubsub = self.redis_conn.pubsub()
        # pubsub.psubscribe('__keyevent@0__:expired')
        #
        # for msg in pubsub.listen():
        #     print(msg)

        self.BLogger = BCloudLogger_Singleton.instance()
        self.info_logger = self.BLogger.get_info_logger()
        self.debug_logger = self.BLogger.get_debug_logger()

    def GetData(self, key):
        try:
            # redis_data=pickle.loads(self.redis_conn.get(key))
            data = self.redis_conn.get(key)

            # redis_data=self.redis_conn.hgetall(key)

            if data is not None:
                # return RedisSessionObject(key,redis_data)
                return json.loads(data)

            else:
                return False
        except Exception as e:

            self.debug_logger.Get_Logger().debug(str(e))
            return False

    def GetAllKeys(self):
        try:
            return self.redis_conn.keys(pattern='*')
        except Exception as e:
            self.debug_logger.Get_Logger().debug(str(e))
            return False

    def SetData(self, key, value):
        if key is not None:
            if self.timeout is not None:
                self.redis_conn.setex(key, self.timeout, value)
            else:
                self.redis_conn.set(key, value)

    def RemoveData(self, key):
        self.redis_conn.delete(key)


class RedisInterface(Singleton.SingletonInstance):
    def __init__(self):
        self.redis_conn_pool = dict()

    def RegistRedisConnection(self, conn_key, db, password, timeout=3600, host='127.0.0.1', port=6379):
        self.redis_conn_pool[conn_key] = ConnectionObject(db, password, timeout, host, port)

    def GetRedisConnection(self, conn_key):
        return self.redis_conn_pool[conn_key]
    # def SetRedis(self,db,password,timeout=3600,host='127.0.0.1',port=6379):
    #     #self.redis_pool = redis.ConnectionPool(host=host,port=port,db=db,password=password)
    #     self.redis_conn = redis.StrictRedis(host=host,port=port,db=db,password=password)
    #     #self.redis_conn=redis.Redis(connection_pool=self.redis_pool)
    #
    #     self.timeout=timeout
    #     self.BLogger = BCloudLogger_Singleton.instance()
    #     self.Log = self.BLogger.get_info_logger()
    #     self.Log = self.BLogger.get_debug_logger()
    # def GetSession(self,key):
    #     try:
    #         #redis_data=pickle.loads(self.redis_conn.get(key))
    #         redis_data = json.loads(self.redis_conn.get(key).decode('utf-8'))
    #     #redis_data=self.redis_conn.hgetall(key)
    #
    #         if redis_data is not None:
    #             #return RedisSessionObject(key,redis_data)
    #             return redis_data
    #         else:
    #             return False
    #     except Exception as e:
    #
    #         self.Log.Get_Logger().debug(str(e))
    #         return False
    #
    # def SetSession(self,key,value):
    #     #self.redis_conn.setex(key,self.timeout,pickle.dumps(value))
    #     self.redis_conn.setex(key, self.timeout, json.dumps(value))
    #
    # def RemoveSession(self,key):
    #     self.redis_conn.delete(key)


class RedisSessionObject():

    def __init__(self, session_key, session_redis):
        self.key = session_key
        self.redis_conn = session_redis

    def __del__(self):
        del (self.key)
