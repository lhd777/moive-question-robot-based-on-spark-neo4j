import jieba
import jieba.analyse
import jieba.posseg

question_dir = "./question_classification.txt"
genre_dir = "./自定义词典/genreDict.txt"
moive_dir = "./自定义词典/movieDict.txt"

with open(question_dir, encoding='utf-8') as f:
    question_dir = f.read().splitlines()
    for i, line in enumerate(question_dir):
        question_dir[i] = line.split(':')[-1]

with open(genre_dir, encoding='utf-8-sig') as f:
    genre_dir = f.read().splitlines()
    for i, line in enumerate(genre_dir):
        genre_dir[i] = line

with open(moive_dir, encoding='utf-8-sig') as f:
    moive_dir = f.read().splitlines()
    for i, line in enumerate(moive_dir):
        moive_dir[i] = line


def dosegment_all(query):
    sentence_seged = jieba.posseg.cut(query)
    outstr = ''
    outch = ''
    for x in sentence_seged:
        if x.word in moive_dir:
            outstr += "{},".format(x.word)
            outch += "{},".format('nm')
        elif x.word in genre_dir:
            outstr += "{},".format(x.word)
            outch += "{},".format('ng')
        else:
            outstr += "{},".format(x.word)
            outch += "{},".format(x.flag)
    outstr = outstr.split(',')
    outch = outch.split(',')
    return outstr, outch


def query_abstract(query):
    str,charact = dosegment_all(query)
    abstract_query = {}
    nrCount = 0
    for ch in charact:
        if ch == 'nm':
            index = charact.index(ch)
            abstract_query[ch] = str[index]
        elif ch == 'nr' and nrCount==0:
            index = charact.index(ch)
            abstract_query[ch] = str[index]
            charact[index]='0'
            nrCount +=1
        elif ch == 'nr' and nrCount==1:
            new_word = 'nnr'
            index = charact.index(ch)
            abstract_query[new_word] = str[index]
        elif ch =='m':
            index = charact.index(ch)
            abstract_query[ch] = str[index]
        elif ch =='ng':
            index = charact.index(ch)
            abstract_query[ch] = str[index]
    return abstract_query


def match_question(prediction, query):
    query = query_abstract(query)
    question = question_dir[prediction]
    if 'nm' in question and 'nm' in query.keys():
        question = question.replace('nm', query['nm'])
    if 'nnr' in question and 'nnr' in query.keys():
        question = question.replace('nnr', query['nnr'])
    if 'nr' in question and 'nr' in query.keys():
        question = question.replace('nr', query['nr'])
    if 'ng' in question and 'ng' in query.keys():
        question = question.replace('ng', query['ng'])
    if 'm' in question and 'm' in query.keys():
        question = question.replace('m', query['m'])
    return question
