from pyspark.ml.classification import NaiveBayes, NaiveBayesModel, DecisionTreeClassifier, DecisionTreeClassificationModel
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession
from pyspark.sql import Row
import re
import os
import jieba
import jieba.posseg
import pickle
import time


data_dir = './question/'
vocabularys = './vocabulary.txt'
genre_dir = "./自定义词典/genreDict.txt"
moive_dir = "./自定义词典/movieDict.txt"

with open(genre_dir, encoding='utf-8-sig') as f:
    genre_dir = f.read().splitlines()
    for i, line in enumerate(genre_dir):
        genre_dir[i] = line

with open(moive_dir, encoding='utf-8-sig') as f:
    moive_dir = f.read().splitlines()
    for i, line in enumerate(moive_dir):
        moive_dir[i] = line


# 对数据进行分词处理，对于电影、类型采用自定义词典
def pre_dosegment(query):
    sentence_seged = jieba.posseg.cut(query)
    outstr = ''
    for x in sentence_seged:
        if x.flag =='nr':
            outstr += "{},".format('nnt')
        elif x.word in moive_dir:
            outstr += "{},".format('nm')
        elif x.word in genre_dir:
            outstr += "{},".format('ng')
        else:
            outstr += "{},".format(x.word)
    outstr = outstr.split(',')
    return outstr


class NaiveBayesModelMe:
    def __init__(self):
        self.vocabularys = []
        self.model_path = "./nb_model"
        # 初始化一个spark的session
        self.spark = SparkSession \
            .builder \
            .appName('my_first_app_name') \
            .getOrCreate()

    def load(self, data_dir, vocabularys):
        # 读取vocabularys文件，构建高频的词库
        with open(vocabularys, encoding='utf-8') as f:
            vocabulary = f.read().splitlines()
            for i, line in enumerate(vocabulary):
                vocabulary[i] = line.split(':')[-1]
        # print(vocabulary[0])
        texts = []
        self.x = []
        self.vocabularys = vocabulary
        # 遍历读取文件夹中的数据文件，共14个文件
        for parent, dirnames, filenames in os.walk(data_dir):
            self.x = []
            for filename in filenames:
                if filename[0] != '.':
                    # print(os.path.join(data_dir, filename))
                    with open(os.path.join(data_dir, filename), encoding='UTF-8') as f:
                        file = f.read().splitlines()
                    texts.append(file)
                    self.x.append(filename)
        document = {}
        # print(len(self.x))
        # 对每一个数据文件中的词进行分词操作，并将其文件名作为字典索引的key
        for i, text in enumerate(texts):
            lines = []
            for line in text:
                word = jieba.cut(line)
                lines.append(' '.join(word))
            document[self.x[i]] = lines

        document_new = {}
        # 对分词后的数据文件的每一行进行和高频词库的匹配，将数据文件的每一行转为一个行向量
        for i in range(len(document)):
            vectors = []
            for line in document[self.x[i]]:
                line = line.split(" ")
                vector = [0 for x in range(len(vocabulary))]
                for word in line:
                    if word in vocabulary:
                        index = vocabulary.index(word)
                        vector[index] = 1
                vectors.append(vector)
            document_new[self.x[i]] = vectors

        # print(len(document_new))
        # 提取数据文件Title中的数字为类别标签,将其保存为训练集的形式
        self.train_data = []
        for i in range(len(document_new)):
            num = re.findall('\d+', self.x[i])
            self.train_data.append((int(num[0]), document_new[self.x[i]]))

        output = open('data.pkl', 'wb')
        pickle.dump(self.train_data, output)
        output = open('vocabulary.pkl', 'wb')
        pickle.dump(self.vocabularys, output)


    def fit(self):

        # 构建一个spark类型的dataframe格式的训练集形式
        pkl_file = open('data.pkl', 'rb')
        train_data = pickle.load(pkl_file)

        df = self.spark.createDataFrame(
            [Row(label=train_data[j][0], weight=0.1, features=Vectors.dense(train_data[j][1][i])) for j in range(14) for
             i in range(len(train_data[j][1]))])

        nb = NaiveBayes(smoothing=1.0, modelType="multinomial", weightCol="weight")
        # nb = DecisionTreeClassifier()
        print("训练正在开始-------------->")
        model = nb.fit(df)
        model.save(self.model_path)

    def test(self, sentence, vocabularys):
        # sentence = ' '.join(jieba.cut(sentence))
        sentence = sentence.split(" ")
        print('句子抽象化后的结果: {}'.format(sentence))
        vector = [0 for x in range(len(vocabularys))]
        for word in sentence:
            if word in vocabularys:
                index = vocabularys.index(word)
                vector[index] = 1

        model = NaiveBayesModel.load(self.model_path)
        # model = DecisionTreeClassificationModel.load(self.model_path)
        test0 = self.spark.createDataFrame([Row(features=Vectors.dense(vector))])
        result = model.transform(test0).head()
        print('The model index is: {}'.format(result.prediction))
        return int(result.prediction)


# jieba.load_userdict(moive_dir)
# pkl_file = open("vocabulary.pkl", "rb")
# vocabulary = pickle.load(pkl_file)
# sentence = '不能说的秘密中参演的演员都有哪些'
# sentence = pre_dosegment(sentence)
# sentence = ' '.join(sentence)
#
# start = time.time()
# model = NaiveBayesModelMe()
# model.load(data_dir=data_dir, vocabularys=vocabularys)
# model.fit()
# model.test(sentence, vocabulary)
# end = time.time()
# time = end - start
# print("测试时间为: {:.4f}".format(time))
