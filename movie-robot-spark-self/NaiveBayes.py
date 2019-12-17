from rdd import *
import numpy as np
import pickle
import jieba
import jieba.posseg
import re
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


class NaiveBayesModel:
    def __init__(self):
        self.vocabularys = []

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
        sc = Context()
        pkl_file = open("vocabulary.pkl", "rb")
        vocabulary = pickle.load(pkl_file)
        pkl_file = open("data.pkl", "rb")
        data = pickle.load(pkl_file)
        train_data = []
        for i in range(len(data)):
            for j in range(len(data[i][1])):
                train_data.append((data[i][0], data[i][1][j]))
        length = len(data)
        myData = sc.parallelize(train_data, numPartitions=2)
        c1 = myData.Map(lambda x: (x[0], 1)).reduceByKey(lambda x, y: x + y).Map(lambda x: (x[0], x[1]/length)).collect()
        p_priority = sorted(c1, key=lambda x: x[0])
        num = myData.Map(lambda x: (x[0], 1)).reduceByKey(lambda x, y: x + y).collect()
        num = sorted(num, key=lambda x: x[0])

        # print(p_priority)
        c2 = myData.Map(lambda x: (x[0], x[1])).reduceByKey(lambda x, y: [x[i] + y[i] for i in range(len(x))]).collect()
        c2 = sorted(c2, key=lambda x: x[0])

        length = len(p_priority)
        p_x_l = np.zeros((length, len(vocabulary)))
        for i, x in enumerate(c2):
            for j in range(len(vocabulary)):
                if c2[i][1][j] == 0:
                    c2[i][1][j] += 1
            c2[i] = (c2[i][0], [np.log(c2[i][1][j] / num[i][1]) for j in range(len(vocabulary))])

        # 检查训练集合的准确度
        acc = 0
        for i in range(len(train_data)):
            p_l_x = []
            for j in range(len(c2)):
                p_ = sum(np.array(c2[j][1]) * train_data[i][1]) + p_priority[j][1]
                p_l_x.append(p_)
            p_l_x = np.array(p_l_x)
            index = np.argmax(p_l_x)
            if index == train_data[i][0]:
                acc += 1
        acc /= len(train_data)
        print('训练集的准确度为{:.1f}'.format(acc*100))

        output = open('independentP.pkl', 'wb')
        pickle.dump(c2, output)
        output = open('priorityP.pkl', 'wb')
        pickle.dump(p_priority, output)

    def test(self, sentence):
        pkl_file = open('independentP.pkl', 'rb')
        independentP = pickle.load(pkl_file)
        pkl_file = open('priorityP.pkl', 'rb')
        priorityP = pickle.load(pkl_file)
        pkl_file = open("vocabulary.pkl", "rb")
        vocabulary = pickle.load(pkl_file)

        jieba.load_userdict(moive_dir)
        sentence = pre_dosegment(sentence)
        sentence = ' '.join(sentence)
        sentence = sentence.split(" ")
        vector = [0 for x in range(len(vocabulary))]
        for word in sentence:
            if word in vocabulary:
                index = vocabulary.index(word)
                vector[index] = 1

        p = []
        for j in range(len(independentP)):
            p_ = sum(np.array(independentP[j][1]) * vector) + priorityP[j][1]
            p.append(p_)
        return np.argmax(np.array(p))


# start = time.time()
# model = NaiveBayesModel()
# model.fit()
# sentence = '不能说的秘密中参演的演员都有哪些'
# label = model.test(sentence)
# end = time.time()
# time = end - start
# print("测试时间为: {:.4f}".format(time))
# print(label)


