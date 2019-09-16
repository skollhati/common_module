# -*- coding: utf-8 -*-
#CSV 파일을 JSON 타입으로 고치기 위한 모듈

import csv

#list 내부의 한글을 인코딩하기 위한 함수(unicode로 인해 일어나는 문제 해결)
def EncodingList(e_list):
    r_list = list(x.decode('euckr').encode('utf-8') for x in e_list)

    return r_list

#CSV의 열 데이터를 사용하기 좋게 column 리스트화 하는 함수
def ConvertRow2List(csv_data):
    axis_data = []

    csv_len = csv_data[2].__len__()

    for x in range(csv_len):
        axis_data.append([])

    for in_csv in csv_data:
        for i in range(csv_len):
            axis_data[i].append(in_csv[i])

    return axis_data

#CSV파일을 열어서 필요한 양식으로 고치는 함수
def OpenCSV(filename):
    f = open('../CSVStore/%s' % filename, 'rb')
    #f = open('./test_data2.csv', 'rb')
    csvReader = csv.reader(f, delimiter=',')

    csv_data = list(csvReader)

    for x in range(0, 3):
        csv_data[x] = EncodingList(csv_data[x])

    axis_data = ConvertRow2List(csv_data[1:])

    return CSV2JSON_LINE(csv_data[0], axis_data)

#재정리된 데이터를 JSON 형태로 변형하는 함수
def CSV2JSON_LINE(title, axis_data):
    chart = {"title": title[0],}
    chart["axis_data"] = dict()

    for axis in range(len(axis_data)):

        if axis == 0:
            chart["axis_data"]["x_axis"] =\
                {
                    "title": axis_data[axis][0],
                    "measure": axis_data[axis][1],
                    "data": axis_data[axis][3:]
                }
        else:
            if axis == 1:
                chart["axis_data"]["y_axis"] = \
                    {
                        "title": axis_data[axis][0],
                        "measure": axis_data[axis][1],
                        "data_set":
                            {
                                axis-1:
                                    {
                                        "data_name":axis_data[axis][2],
                                        "data": InsertNoneData(axis_data[axis][3:])
                                    }
                            }
                    }
            else:
                chart["axis_data"]["y_axis"]["data_set"][axis-1]=\
                    {
                        "data_name":axis_data[axis][2],
                        "data": InsertNoneData(axis_data[axis][3:])
                    }

    return chart

#data에 GAP이 생길경우 None을 입력하기 위해 만들었지만 제대로 실행X 추후 개발이 필요함
def InsertNoneData(data_list):
    temp_list = list()
    for x in data_list:
        if x == u"None":
            temp_list.append(None)
        else:
            temp_list.append(x)
    return temp_list


