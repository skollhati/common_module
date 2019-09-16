# coding: utf-8
import json
from BCloudModule.LogModule_Singleton import BCloudLogger_Singleton
from sqlalchemy import create_engine, exc
from sqlalchemy.pool import QueuePool
import BCloudModule.BModule_Singleton as Singleton

#unicode나 형식에 맞지 않는 데이터를 각각 형식에 맞게 변형하는 함수
#column은 형식이 홑따옴표나 겹따옴표가 필요없는 문자 그대로 들어가야함
#value는 문자열은 홑따옴표 숫자, 기타 데이터류는 따옴표 없이 들어가야함
#문자열 중 unicode형식으로 된 문자열은 utf-8로 인코딩하거나 "u'"를 Replace로 제거해주는 함수

def QueryConverter(s_column, s_value):

    query_str = " "

    if s_value == None and s_column != None:

        query_str += ", ".join(s_column)

        return query_str

    elif s_value != None and s_column == None:

        for s in s_value:
            if isinstance(s, str):
            #if type(s) == type('a'):
                if s[:10] =='CONVERT_TZ':
                    query_str += s+", "
                else:
                    query_str += "'" + s + "', "
            elif isinstance(s,u''):
            #elif type(s) == type(u''):
                dict_str=s.encode('utf-8')
                if dict_str[:10] =='CONVERT_TZ':
                    query_str += dict_str.replace('\'', '\"') +", "
                else:
                    query_str += "'" + dict_str.replace('\'', '\"') + "', "
            elif type(s) == type(dict()):
                dict_str = s.__str__().replace('u\'', '\"')
                query_str += "'"+dict_str.replace('\'', '\"') + "', "
            else:
                query_str += s.__str__() + ", "
            # if type(s) == type(dict()) or type(s) == type(u''):
            #     query_str += "'" + s.__str__().encode('utf-8').replace('\'', '\"') + "', "
            # elif type(s) == type('a'):
            #     query_str += "'" + s + "', "
            # else:
            #     query_str += s.__str__() + ", "
        return query_str[:-2]

    else:
        q_str = []
        if type(s_column) == type(list()):
            it = iter(s_value)
            for col in s_column:
                v = it.next()
                if type(v) == type('c'):
                    q_str.append(col + "=\'" + v + "\'")
                elif type(v) == type(dict()):
                    dict_str = v.__str__().replace('u\'', '\"')
                    v = dict_str.replace('\'', '\"')
                    q_str.append(col + "=\'" + v + "\'")
                else:
                    q_str.append(col + "='" + v.__str__()+"'")
        else:
            if type(s_value) == type('c'):
                q_str.append(s_column + "=\'" + s_value + "\'")
            else:
                q_str.append(s_column + "=" + s_value.__str__())

        return ", ".join(q_str)


#DB모듈 객체...시간 될때 싱글톤으로 교체하는 방안..생각 중
#커넥션풀과 비슷하게...
class DBObject(Singleton.SingletonInstance):

    def __init__(self):
        pass

    # config.json 파일을 이용해서 db정보 로드
    def DisConnectDB(self,cursor):
        cursor.close()

    def SetConfig(self,config):
        self.BLogger = BCloudLogger_Singleton.instance()
        self.info_logger = self.BLogger.get_info_logger()
        self.debug_logger = self.BLogger.get_debug_logger()

        try:
            self.db_type = config["type"]

            if self.db_type == "mysql":
                self.db_EngineInfo = "mysql+mysqlconnector://%s:%s@%s:%s/%s?charset=utf8" % (
                    config["usr"], config["pwd"], config["host"], config["port"], config["dbname"])

            elif self.db_type == "postgresql":
                self.db_EngineInfo = "postgresql+psycopg2://%s:%s@%s:%s/%s?charset=utf8" % (
                    config["usr"], config["pwd"], config["host"], config["port"], config["dbname"])

            self.conn = create_engine(self.db_EngineInfo, pool_size=20, max_overflow=10,pool_recycle=20000)
            self.db_error = exc.DatabaseError

        except Exception as e:
            self.debug_logger.Get_Logger().error(e.__str__())
            self.info_logger.Get_Logger().error("ReConnect Error -  ReConnect Failed")

    def DBConnect(self):
        try:

            return self.conn.connect()

        except Exception as e:
            self.debug_logger.Get_Logger().error(e.__str__())
            self.info_logger.Get_Logger().error("ReConnect Error -  ReConnect Failed")


    # #재연결
    # def ReConnect(self):
    #     try:
    #         self.cur = self.conn.connect()
    #         self.db_error = exc.DatabaseError
    #         return True
    #     except Exception as e:
    #         self.debug_logger.Get_Logger().error(e.__str__())
    #         self.info_logger.Get_Logger().error("ReConnect Error -  ReConnect Failed")
    #         return False


    #select column도 지정 할 수 있게! - 나중에 개인 DB 라이브러리로 사용할 수 있도록..
    def SelectDB(self, s_column, s_value,table_name,query_option =""):
        #" ORDER BY num"
        try:
            cursor=self.DBConnect()
            if type(s_value) == type(1):
                query_str = "SELECT * from " + table_name + " where " + s_column + "= %s" % s_value +' '+ query_option
            else:
                query_str = "SELECT * from " + table_name + " where " + s_column + "= \'%s\'" % s_value+' '+query_option
            self.debug_logger.Get_Logger().debug("DB_Module :" + query_str)
            result = cursor.execute(query_str).cursor.fetchall()
            self.DisConnectDB(cursor)
            return result
        except exc.DBAPIError as a:
            self.debug_logger.Get_Logger().error("DB Connect Error:" +a.__str__())
            self.DisConnectDB(cursor)
            if a.connection_invalidated:
                self.debug_logger.Get_Logger().error("try reconnect DB")

                self.SelectDB(s_column,s_value,table_name,query_option)

        except self.db_error as e:
            self.info_logger.Get_Logger().error("Select Failed :"+ e.__str__())
            self.debug_logger.Get_Logger().error("Select Failed :" + e.__str__())
            self.DisConnectDB(cursor)
            return False

    def CallStoredProcedure_GetColumnName(self, call_sp, argslist):
        try:
            rconn = self.conn.raw_connection()
            cursor = rconn.cursor()
            self.debug_logger.Get_Logger().debug("DB_Module Call SP:" + call_sp)
            cursor.callproc(call_sp, argslist)

            # result = next(cursor.stored_results()).fetchall()
            # result = list(cursor.fetchall())

            sresult = list(cursor.stored_results())
            if len(sresult) > 0:
                result = sresult[0].fetchall()
            else:
                result = True

            cursor.close()
            rconn.close()
            return [self.GetColumnNames(sresult),result]
        except exc.DBAPIError as e:
            self.debug_logger.Get_Logger().error("DB_Module Call SP:" + e.__str__())
            self.DisConnectDB(cursor)
            if e.connection_invalidated:
                self.debug_logger.Get_Logger().error("try reconnect DB")
                self.CallStoredProcedure(call_sp, argslist)

            else:
                self.info_logger.Get_Logger().error("DB_Module Call SP:" + e.__str__())
                self.debug_logger.Get_Logger().error("DB_Module Call SP:" + e.__str__())

                return False

        except Exception as e:
            self.info_logger.Get_Logger().error("DB_Module Call SP:" + e.__str__())
            self.debug_logger.Get_Logger().error("DB_Module Call SP:" + e.__str__())
            self.DisConnectDB(cursor)
            return False
    def GetColumnNames(self,cursor):
        return list(cursor)[0].column_names
    def CallStoredProcedureInCommit(self, call_sp, argslist=[]):
        rconn = self.conn.raw_connection()
        cursor = rconn.cursor()
        try:

            # self.debug_logger.Get_Logger().debug("DB_Module Call SP:" + call_sp)
            cursor.callproc(call_sp, argslist)

            # result = next(cursor.stored_results()).fetchall()

            cursor.close()
            rconn.close()

        except exc.DBAPIError as e:
            self.debug_logger.Get_Logger().error("DB_Module Call SP:" + e.__str__())
            self.DisConnectDB(cursor)
            if e.connection_invalidated:
                self.debug_logger.Get_Logger().error("try reconnect DB")
                self.CallStoredProcedure(call_sp, argslist)

            else:
                self.info_logger.Get_Logger().error("DB_Module Call SP:" + e.__str__())
                self.debug_logger.Get_Logger().error("DB_Module Call SP:" + e.__str__())

                return False

        except Exception as e:
            self.info_logger.Get_Logger().error("DB_Module Call SP:" + e.__str__())
            self.debug_logger.Get_Logger().error("DB_Module Call SP:" + e.__str__())
            self.DisConnectDB(cursor)
            return False

    def CallStoredProcedure(self, call_sp, argslist, res_errmsg=False):
        rconn = self.conn.raw_connection()
        cursor = rconn.cursor()
        try:
            self.debug_logger.Get_Logger().debug("DB_Module Call SP:" + call_sp)
            cursor.callproc(call_sp, argslist)
            sresult = list(cursor.stored_results())
            if len(sresult) > 0:
                result = sresult[0].fetchall()
            else:
                result = True
            cursor.close()
            rconn.close()
            return result

        except exc.DBAPIError as e:
            self.debug_logger.Get_Logger().error("DB_Module Call SP:" + e.__str__())
            self.DisConnectDB(cursor)
            if e.connection_invalidated:
                self.debug_logger.Get_Logger().error("try reconnect DB")
                self.CallStoredProcedure(call_sp,argslist)

            else:
                self.info_logger.Get_Logger().error("DB_Module Call SP:" + e.__str__())
                self.debug_logger.Get_Logger().error("DB_Module Call SP:" + e.__str__())
                return False

        except Exception as e:
            self.info_logger.Get_Logger().error("DB_Module Call SP:" + e.__str__())
            self.debug_logger.Get_Logger().error("DB_Module Call SP:" + e.__str__())
            self.DisConnectDB(cursor)
            if res_errmsg:
                return False, e
            return False

    def sp_get_multiple_results(self, call_sp, argslist, res_errmsg=False):
        rconn = self.conn.raw_connection()
        cursor = rconn.cursor()
        try:
            self.debug_logger.Get_Logger().debug("DB_Module Call SP:" + call_sp)
            cursor.callproc(call_sp, argslist)
            sresult = list(cursor.stored_results())
            result = []
            if len(sresult) > 0:
                for each in sresult:
                    result.append(each.fetchall())
            else:
                pass
            cursor.close()
            rconn.close()
            return result

        except exc.DBAPIError as e:
            self.debug_logger.Get_Logger().error("DB_Module Call SP:" + e.__str__())
            self.DisConnectDB(cursor)
            if e.connection_invalidated:
                self.debug_logger.Get_Logger().error("try reconnect DB")
                self.CallStoredProcedure(call_sp,argslist)

            else:
                self.info_logger.Get_Logger().error("DB_Module Call SP:" + e.__str__())
                self.debug_logger.Get_Logger().error("DB_Module Call SP:" + e.__str__())
                return False

        except Exception as e:
            self.info_logger.Get_Logger().error("DB_Module Call SP:" + e.__str__())
            self.debug_logger.Get_Logger().error("DB_Module Call SP:" + e.__str__())
            self.DisConnectDB(cursor)
            if res_errmsg:
                return False, e
            return False

    def ManualQueryDB(self, query_str, res_errmsg = False):
        #" ORDER BY num"
        cursor = self.DBConnect()
        try:
            self.debug_logger.Get_Logger().debug("DB_Module :" + query_str)
            result = cursor.execute(query_str).cursor.fetchall()
            self.DisConnectDB(cursor)
            return result

        except exc.DBAPIError as a:
            self.debug_logger.Get_Logger().error("DB Connect Error:" + a.__str__())
            self.DisConnectDB(cursor)
            if a.connection_invalidated:
                self.debug_logger.Get_Logger().error("try reconnect DB")
                self.ManualQueryDB(query_str)

        except Exception as e:
            self.info_logger.Get_Logger().error("Manual Failed :"+ e.__str__())
            self.debug_logger.Get_Logger().error("Manual Failed :" + e.__str__())
            self.DisConnectDB(cursor)
            if res_errmsg:
                return False, e
            return False

    def ManualInsertQueryDB(self, query_str):
        # " ORDER BY num"
        cursor = self.DBConnect()
        try:

            self.debug_logger.Get_Logger().debug("DB_Module :" + query_str)

            result = cursor.execute(query_str)
            self.DisConnectDB(cursor)
            return result
        except exc.DBAPIError as e:
            self.debug_logger.Get_Logger().error("DB Connect Error:" + e.__str__())
            self.DisConnectDB(cursor)
            if e.connection_invalidated:
                self.debug_logger.Get_Logger().error("try reconnect DB")

                self.ManualQueryDB(query_str)
            else:
                self.info_logger.Get_Logger().error("Manual Failed :" + e.__str__())
                self.debug_logger.Get_Logger().error("Manual Failed :" + e.__str__())
                return False

        except Exception as e:
            self.info_logger.Get_Logger().error("Manual Failed :" + e.__str__())
            self.debug_logger.Get_Logger().error("Manual Failed :" + e.__str__())
            self.DisConnectDB(cursor)
            return False

    #s_value 가 튜플인 경우는 하나의 데이터
    #s_value가 리스트인 경우는 여러 데이터를 한번에 insert
    def InsertDB(self,s_column,s_value,table_name):
        cursor = self.DBConnect()
        try:

            #tuple list 구분 명확히 다시 검토할 것
            if type(s_value)==type(tuple()):
                query_str = "INSERT INTO "+ table_name +" ("+QueryConverter(s_column,None)+") VALUES (" + QueryConverter(None,s_value)+")"
            elif type(s_value) == type(list()):
                data_values=list()
                for d_list in s_value:
                    data_values.append(QueryConverter(None,d_list))

                values = "), (".join(data_values)
                query_str = "INSERT IGNORE INTO " + table_name + " (" + QueryConverter(s_column,
                                                                                None) + ") VALUES (" + values+")"


            self.debug_logger.Get_Logger().debug("DB_Module :" + query_str)
            cursor.execute(query_str)

            self.DisConnectDB(cursor)

            return True
        except exc.DBAPIError as a:
            self.debug_logger.Get_Logger().error("DB Connect Error:" + a.__str__())
            self.DisConnectDB(cursor)

            if a.connection_invalidated:
                self.debug_logger.Get_Logger().error("try reconnect DB")
                self.InsertDB(s_column,s_value,table_name)


        except self.db_error as e:
            self.info_logger.Get_Logger().error("Insert Failed :" + e.__str__())
            self.debug_logger.Get_Logger().error("Insert Failed :" + e.__str__())
            self.DisConnectDB(cursor)
            return False

    #s_value 대입값 p_where 조건값
    #[0] col [1] val [2] where data ( 0 - where col 1 - where data)
    def UpdateDB(self,s_column,s_value,p_where,table_name):
        cursor = self.DBConnect()
        try:

            q_set = QueryConverter(s_column,s_value)

            if str==type(p_where):
                query_str = "UPDATE " + table_name + " set " + q_set + " WHERE " + p_where
            elif type(p_where[1]) == type('a'):
                query_str = "UPDATE "+table_name +" set "+ q_set+ " WHERE "+ p_where[0]+"=" + p_where[1]
            else:
                query_str = "UPDATE " + table_name + " set " + q_set + " WHERE " + p_where[0] + "=" +  p_where[1].__str__()
            print(query_str)
            #self.Log.Get_Logger().debug("DB_Module :" + query_str)
            cursor.execute(query_str)
            
            self.DisConnectDB(cursor)
            return True
        except exc.DBAPIError as a:
            self.debug_logger.Get_Logger().error("DB Connect Error:" +a.__str__())
            self.DisConnectDB(cursor)
            if a.connection_invalidated:
                self.debug_logger.Get_Logger().error("try reconnect DB")
                self.UpdateDB(s_column,s_value,p_where,table_name)

        except self.db_error as e:

            self.debug_logger.Get_Logger().error("Update Failed :"+ e.__str__())
            self.info_logger.Get_Logger().error("Update Failed :"+ e.__str__())
            self.DisConnectDB(cursor)

            return False

    def ManualQueryInTransaction(self,query_str_list):
        cursor = self.DBConnect()
        try:

            trans = cursor.begin()

            for query_str in query_str_list:
                cursor.execute(query_str)
            trans.commit()
            result =True
        except Exception as e:
            self.info_logger.Get_Logger().error("Manual Failed :" + e.__str__())
            result = False

        finally:
            self.DisConnectDB()
            return result
    