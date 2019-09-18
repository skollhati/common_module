# coding: utf-8
import json
import logging
import os
import datetime
import logging.handlers
import logging.config
import ast
import personal_api.BModule_Singleton as Singleton
logfile_max = 10 * 1024 *1024

#싱글톤 타입의 로거
class BCloudLogger_Singleton(Singleton.SingletonInstance):

    def __init__(self):
        pass

    def LoggerConfig(self,log_config):
        debug_config = log_config["DEBUG"]
        info_config = log_config["INFO"]
        post_info_config = log_config["POSTINFO"]
        log_path = os.path.dirname(os.path.realpath(__file__).replace("BCloudModule",""))+log_config["LOG_PATH"]

        self.__debug_logger = LoggerObject(log_path, "DebugLog", logging.DEBUG, **debug_config)
        self.__info_logger = LoggerObject(log_path, "InfoLog", logging.INFO, **info_config)
        self.__post_info_logger = LoggerObject(log_path, "PostLog", logging.INFO, **post_info_config)

    def get_info_logger(self):
        return self.__info_logger

    def get_debug_logger(self):
        return self.__debug_logger

    def get_post_logger(self):
        return self.__post_info_logger

class BCloudLogger:

    def __init__(self,log_config,log_path):
        self.__logger = LoggerObject(log_path, log_config['path_name'], logging.DEBUG, **log_config)

    def get_logger(self):
        return self.__logger


#BCloud Logger가 가질 Logger 객체 클래스
#월 / 일의 변동에 따라 저장 폴더 및 로그파일 생성하도록 코딩함
class LoggerObject:
    def __init__(self, log_path, logger_name, logger_Level, format_str, path_name, stream):
        self.logger_obj_path = log_path
        self.logger_file_name = path_name
        self.logger_obj_format = format_str
        self.logger_obj = logging.getLogger(logger_name)
        self.logger_obj.setLevel(logger_Level)

        self.monthDate = datetime.datetime.now().strftime('%Y%m')
        self.nowDate = datetime.datetime.now().strftime('%Y%m%d')

        self.log_formatter = logging.Formatter(self.logger_obj_format)

        if not os.path.exists("%s/%s/"%(self.logger_obj_path,self.monthDate)):
            os.mkdir("%s/%s/" % (self.logger_obj_path,self.monthDate))

        self.log_fileHandler = logging.handlers.RotatingFileHandler(
            '%s/%s/%s_%s.log' % (self.logger_obj_path,self.monthDate, self.logger_file_name,self.nowDate),
            maxBytes=logfile_max, backupCount=10)

        self.log_fileHandler.setFormatter(self.log_formatter)
        self.logger_obj.addHandler(self.log_fileHandler)

        if ast.literal_eval(stream):
            self.log_streamHandler = logging.StreamHandler()
            self.log_streamHandler.setFormatter(self.log_formatter)
            self.logger_obj.addHandler(self.log_streamHandler)

    #logger 사용시 날짜 확인용 함수 - 확인 후 폴더 및 로그 파일 재생성
    def Get_Logger(self):

        r_monthDate = datetime.datetime.now().strftime('%Y%m')
        r_nowDate = datetime.datetime.now().strftime('%Y%m%d')

        # if not os.path.exists("../Log/%s/" % r_monthDate):
        # 로거 생성 월이 달라지면 로그 폴더 내부 월별 폴더 생성
        # 기존에 폴더 확인하는 함수는 디스크를 읽는것보다 메모리에 저장된 날짜 비교가 낫다고 판단...
        if not os.path.isdir("%s/%s/" % (self.logger_obj_path, r_monthDate)):
            os.mkdir("%s/%s/" % (self.logger_obj_path, r_monthDate))
            # self.monthDate = r_monthDate

        # if not os.path.exists('../Log/%s/Chart_info_%s.log' % (monthDate, nowDate)):
        # 로거 생성시 일자와 현 일자가 달라질 경우 파일 핸들러를 재생성 등록
        # if self.nowDate != r_nowDate:
        #     self.nowDate = r_nowDate
        if not os.path.exists(
                        '%s/%s/%s_%s.log' % (self.logger_obj_path, r_monthDate, self.logger_file_name, r_nowDate)):
            for x in self.logger_obj.handlers:
                if x is self.log_fileHandler:
                    self.logger_obj.handlers.remove(x)



            self.log_fileHandler = logging.handlers.RotatingFileHandler(
                '%s/%s/%s_%s.log' % (self.logger_obj_path,r_monthDate, self.logger_file_name,r_nowDate),
                maxBytes=logfile_max, backupCount=10)

            self.log_formatter = logging.Formatter(self.logger_obj_format)
            self.log_fileHandler.setFormatter(self.log_formatter)

            self.logger_obj.addHandler(self.log_fileHandler)
        return self.logger_obj




    # 월,일 변경에 따른 로그 폴더 및 파일 갱신
    # 지금 로그찍을 때마다 확인 중이라 약간 비효율적이라 느껴짐...다르게 바꿀수 있는 방법이 있을까?
    # 현재 파일 크기에 따라 새로 파일 생성하는 것은 RotatingFileHandler를 통해서 가능해졌음...
    # 시간대에 따라 바꿀 수 있는 방법은 뭐가 없을까?
