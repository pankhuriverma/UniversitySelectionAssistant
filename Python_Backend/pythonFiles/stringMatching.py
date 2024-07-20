import Levenshtein as lev
import csv
import pandas as pd
import math
import mysql.connector
from mysql.connector import Error
import ngram
import nltk
from nltk.corpus import stopwords
nltk.download('stopwords')

databaseTask2 = 'info_integration_task2' 
username = 'root' 
password = 'mysql'
databaseTask4 = 'II_T4'
# datasource1 = list(pd.read_csv("universities_ranking.csv",nrows=1).columns)
# print(datasource1)

# datasource2 = list(pd.read_csv("2020-QS-World-University-Rankings.csv",nrows=1).columns)
# print(datasource2)

# datasource3 = list(pd.read_excel("World University Rankings 2019-20.xlsx",nrows=1).columns)
# print(datasource3)

def match_schema():
    final_mapping = []
    for i in datasource3:
        max_score_Ratio = []
        for j in datasource1:
            ratio = lev.ratio(i.lower(),j.lower())
            temp = {'i' : i,'j': j, 'ratio': ratio}
            max_score_Ratio.append(temp)
        max = max_score_Ratio[0]
        for val in max_score_Ratio:
            if(val['ratio'] > max['ratio']):
                max = val
        final_mapping.append(max)

    print(final_mapping)

def duplicate_uni():
    try:
        conn = mysql.connector.connect(host='localhost',database=databaseTask2,user=username,password=password)
        conn2 = mysql.connector.connect(host='localhost',database=databaseTask4,user=username,password=password)
        cursor = conn.cursor()
        cursor2 = conn2.cursor()
        cursor.execute("select group_concat(l.uni_key SEPARATOR'::'),group_concat(u.institution SEPARATOR'::'),count(*) c from location_task3 l,uni_data u where l.uni_key = u.uni_key group by l.geohash having c = 2")
        rv = cursor.fetchall()
        for sqlresult in rv:
            uni_names = sqlresult[1].split('::')
            uni_keys = sqlresult[0].split('::')
            print(uni_names)
            checkSimilarity(uni_names,uni_keys)
            #     duplicate_insert = cursor.fetchall()
                # for di in duplicate_insert :
                # cursor.execute("insert into duplicate_uni values("+str(duplicate_insert[0][0])+",\""+str(duplicate_insert[0][1])+"\","+str(duplicate_insert[1][0])+",\""+str(duplicate_insert[1][1])+"\")")
                # conn.commit()
            # if(checkJaccardSim(uni_names)):
            #     print('---')
                # query = "insert into duplicate_uni(uni_key,institution) values(%s,%s)"
                # cursor.execute(query,tuple(uni_names))
                # conn.commit()
    except Error as e:
        print("SQL Error: "+str(e))
        return None
    conn.close()
    conn2.close()

def checkSimilarity(uni_names,uni_keys):
    index_i = 0
    index_j = 1
    unique_keys = set()
    while index_i < len(uni_names) - 1:
        while index_j < len(uni_names):
            arr1 = [word for word in uni_names[index_i].split(" ") if word not in set(stopwords.words('english'))]
            arr2 = [word for word in uni_names[index_j].split(" ") if word not in set(stopwords.words('english'))]
            str1 = " ".join(arr1)
            str2 = " ".join(arr2)
            str3 = "".join(arr1)
            str4 = "".join(arr2)
            
            print(str1 + "AND" + str2)
            # levenshtein ration
            distance = lev.ratio(str1.lower(),str2.lower())

            # ngram distance
            ngramDis = ngram.NGram.compare(str1,str2,N=3)
            print("NGRAM:" + str(ngramDis))
            print(distance)

            # jaccard similarity measure
            intersection = len(set(str1).intersection(str2))
            union = len(set(str1)) + len(set(str2)) - intersection

            jaccardIndex = float(intersection) / union
            print("JACCARD:" + str(jaccardIndex))
            if(distance > 0.85 and ngramDis > 0.70):
                print("**************** ITS A MATCH ***************")
                # unique_keys.add(uni_keys[index_j])
                # unique_keys.add(uni_keys[index_i])
                # uni_keys.pop(index_j)
                # uni_names.pop(index_j)
                # remove_duplicates(unique_keys)
                insert_uni_data(uni_keys,True)
            elif(jaccardIndex > 0.85):
                print("**************** ITS A MATCH ***************")
                insert_uni_data(uni_keys,True)
                # remove_duplicates(unique_keys)
            else:
                insert_uni_data(uni_keys,False)
            index_j = index_j + 1
        index_i = index_i + 1

def remove_duplicates(keys):
    try:
        conn = mysql.connector.connect(host='localhost',database=databaseTask2,user=username,password=password)
        cursor = conn.cursor()
        cursor.execute("select uni_key,institution from uni_data where uni_key in ('+"+str(keys.pop())+"','"+str(keys.pop())+"')")
        duplicate_insert = cursor.fetchall()
        cursor.execute("insert into duplicate_uni values("+str(duplicate_insert[0][0])+",\""+str(duplicate_insert[0][1])+"\","+str(duplicate_insert[1][0])+",\""+str(duplicate_insert[1][1])+"\")")
        conn.commit()
    except Error as e:
        print("SQL Error: "+str(e))
        return None
    conn.close()

def insert_uni_data(keys,duplicate):
    try:
        conn = mysql.connector.connect(host='localhost',database=databaseTask4,user=username,password=password)
        cursor = conn.cursor()
        if(duplicate == False):
            query = "insert into uni_data select * from info_integration_task2.uni_data where uni_key in('"+keys.pop()+"','"+keys.pop()+"')"
        else:
            query = "insert into uni_data select uni_key,cast(AVG(ranking) as SIGNED) \
                as ranking,institution,country,MAX(size) as size,MAX(status) as status,MAX(num_stu) \
                as num_stu,MAX(stu_staff_rt) as stu_staff_rt,MAX(intl_stu_per) as intl_stu_per,MAX(gender_rt) \
                as gender_rt,AVG(overall_score) as overall_score from info_integration_task2.uni_data where uni_key in ('" \
                +keys.pop()+"','"+keys.pop()+"')"
        cursor.execute(query)
        conn.commit()
    except Error as e:
        print("SQL Error: "+str(e))
        return None
    conn.close()

if __name__ == '__main__':
    duplicate_uni()





# datasource QS and World Ranking
# [{'i': 'Rank in 2020', 'j': 'World Rank', 'ratio': 0.36363636363636365}, {'i': 'Rank in 2019', 'j': 'World Rank', 'ratio': 0.36363636363636365}, {'i': 'Institution Name', 'j': 'Institution', 'ratio': 0.8148148148148148}, {'i': 'Country', 'j': 'Score', 'ratio': 0.5}, {'i': 'Classification', 'j': 'Location', 'ratio': 0.6363636363636364}, {'i': 'Academic Reputation', 'j': 'Quality\xa0of Education', 'ratio': 0.5128205128205129}, {'i': 'Employer Reputation', 'j': 'Location', 'ratio': 0.5185185185185186}, {'i': 'Faculty Student', 'j': 'Alumni Employment', 'ratio': 0.4375}, {'i': 'Citations per Faculty', 'j': 'Quality\xa0of Faculty', 'ratio': 0.5641025641025641}, {'i': 'International Faculty', 'j': 'National Rank', 'ratio': 0.5882352941176471}, {'i': 'International Students', 'j': 'National Rank', 'ratio': 0.5714285714285715}, {'i': 'Overall Score', 'j': 'Score', 'ratio': 0.5555555555555556}, {'i': 'SIZE', 'j': 'Score', 'ratio': 0.4444444444444444}, {'i': 'FOCUS', 'j': 'Location', 'ratio': 0.3076923076923077}, {'i': 'RESEARCH INTENSITY', 'j': 'Research Performance', 'ratio': 0.5789473684210527}, {'i': 'AGE', 'j': 'Score', 'ratio': 0.25}, {'i': 'STATUS', 'j': 'Institution', 'ratio': 0.4705882352941177}, {'i': 'SCORE', 'j': 'Score', 'ratio': 1.0}, {'i': 'RANK', 'j': 'World Rank', 'ratio': 0.5714285714285715}, {'i': 'SCORE.1', 'j': 'Score', 'ratio': 0.8333333333333333}, {'i': 'RANK.1', 'j': 'World Rank', 'ratio': 0.5}, {'i': 'SCORE.2', 'j': 'Score', 'ratio': 0.8333333333333333}, {'i': 'RANK.2', 'j': 'World Rank', 'ratio': 0.5}, {'i': 'SCORE.3', 'j': 'Score', 'ratio': 0.8333333333333333}, {'i': 'RANK.3', 'j': 'World Rank', 'ratio': 0.5}, {'i': 'SCORE.4', 'j': 'Score', 'ratio': 0.8333333333333333}, {'i': 'RANK.4', 'j': 'World Rank', 'ratio': 0.5}, {'i': 'SCORE.5', 'j': 'Score', 'ratio': 0.8333333333333333}, {'i': 'RANK.5', 'j': 'World Rank', 'ratio': 0.5}, {'i': 'Unnamed: 29', 'j': 'National Rank', 'ratio': 0.33333333333333326}]
# datasource QS and university_ranking
# [{'i': 'Rank in 2020', 'j': 'ranking', 'ratio': 0.631578947368421}, {'i': 'Rank in 2019', 'j': 'ranking', 'ratio': 0.631578947368421}, {'i': 'Institution Name', 'j': 'title', 'ratio': 0.38095238095238093}, {'i': 'Country', 'j': 'location', 'ratio': 0.4}, {'i': 'Classification', 'j': 'location', 'ratio': 0.6363636363636364}, {'i': 'Academic Reputation', 'j': 'gender ratio', 'ratio': 0.5161290322580645}, {'i': 'Employer Reputation', 'j': 'gender ratio', 'ratio': 0.5806451612903225}, {'i': 'Faculty Student', 'j': 'perc intl students', 'ratio': 0.6060606060606061}, {'i': 'Citations per Faculty', 'j': 'location', 'ratio': 0.41379310344827586}, {'i': 'International Faculty', 'j': 'gender ratio', 'ratio': 0.4242424242424242}, {'i': 'International Students', 'j': 'perc intl students', 'ratio': 0.7}, {'i': 'Overall Score', 'j': 'perc intl students', 'ratio': 0.3870967741935484}, {'i': 'SIZE', 'j': 'title', 'ratio': 0.4444444444444444}, {'i': 'FOCUS', 'j': 'location', 'ratio': 0.3076923076923077}, {'i': 'RESEARCH INTENSITY', 'j': 'perc intl students', 'ratio': 0.5555555555555556}, {'i': 'AGE', 'j': 'ranking', 'ratio': 0.4}, {'i': 'STATUS', 'j': 'number students', 'ratio': 0.38095238095238093}, {'i': 'SCORE', 'j': 'location', 'ratio': 0.3076923076923077}, {'i': 'RANK', 'j': 'ranking', 'ratio': 0.7272727272727272}, {'i': 'SCORE.1', 'j': 'location', 'ratio': 0.2666666666666667}, {'i': 'RANK.1', 'j': 'ranking', 'ratio': 0.6153846153846154}, {'i': 'SCORE.2', 'j': 'location', 'ratio': 0.2666666666666667}, {'i': 'RANK.2', 'j': 'ranking', 'ratio': 0.6153846153846154}, {'i': 'SCORE.3', 'j': 'location', 'ratio': 0.2666666666666667}, {'i': 'RANK.3', 'j': 'ranking', 'ratio': 0.6153846153846154}, {'i': 'SCORE.4', 'j': 'location', 'ratio': 0.2666666666666667}, {'i': 'RANK.4', 'j': 'ranking', 'ratio': 0.6153846153846154}, {'i': 'SCORE.5', 'j': 'location', 'ratio': 0.2666666666666667}, {'i': 'RANK.5', 'j': 'ranking', 'ratio': 0.6153846153846154}, {'i': 'Unnamed: 29', 'j': 'number students', 'ratio': 0.3076923076923077}]
# datasource university_ranking and World Ranking
# [{'i': 'World Rank', 'j': 'ranking', 'ratio': 0.4705882352941177}, {'i': 'Institution', 'j': 'location', 'ratio': 0.4210526315789474}, {'i': 'Location', 'j': 'location', 'ratio': 1.0}, {'i': 'National Rank', 'j': 'location', 'ratio': 0.4761904761904762}, {'i': 'Quality\xa0of Education', 'j': 'location', 'ratio': 0.5714285714285715}, {'i': 'Alumni Employment', 'j': 'number students', 'ratio': 0.375}, {'i': 'Quality\xa0of Faculty', 'j': 'students staff ratio', 'ratio': 0.31578947368421056}, {'i': 'Research Performance', 'j': 'gender ratio', 'ratio': 0.375}, {'i': 'Score', 'j': 'location', 'ratio': 0.3076923076923077}]

