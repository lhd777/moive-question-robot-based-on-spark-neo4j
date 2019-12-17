from py2neo import Graph
from match import *


class predict():
    def __init__(self, tg):
        # NoSQL数据库接口
        self.tg = tg

    def run(self, query, question):
        if question == 0:
            return self.score(query)
        if question == 1:
            return self.on_show(query)
        if question == 2:
            return self.gene(query)
        if question == 3:
            return self.moive_introduction(query)
        if question == 4:
            return self.actor_list(query)
        if question == 5:
            return self.actor_introduction(query)
        if question == 6:
            return self.actor_moive_gene(query)
        if question == 7:
            return self.actor_moive_total(query)
        if question == 8:
            return self.score_more(query)
        if question == 9:
            return self.score_less(query)
        if question == 10:
            return self.actor_gene(query)
        if question == 11:
            return self.actor_a_b(query)
        if question == 12:
            return self.act_num(query)
        if question == 13:
            return self.birthday(query)

    def score(self, query):
        query = query.split(" ")
        result = self.tg.run("match(n:Movie) where n.title= $title return n.rating", title=query[0]).to_data_frame()
        if result.empty == False:
            return "该电影的评分为：" + str(result.iat[0,0])
        else:
            return "抱歉，建议您自己百度"

    def on_show(self, query):
        query = query.split(" ")
        result = self.tg.run("match(n:Movie) where n.title= $title return n.releasedate", title=query[0]).to_data_frame()
        if result.empty == False:
            return "该电影上映时间为：" + result.iat[0,0]
        else:
            return "抱歉，建议您自己百度"

    def gene(self, query):
        query = query.split(" ")
        result = self.tg.run("match(n:Movie)-[r:is]->(b:Genre) where n.title=$title return b.name", title=query[0]).to_data_frame()
        word = ""
        for i in range(len(result)):
            word += result.iat[i,0] + ","
        if result.empty == False:
            return "该电影的类型是：" + word
        else:
            return "抱歉，建议您自己百度"

    def moive_introduction(self, query):
        query = query.split(" ")
        result = self.tg.run("match(n:Movie) where n.title =$title return n.introduction",
                        title=query[0]).to_data_frame()
        if result.empty == False:
            return result.iat[0,0]
        else:
            return "抱歉，建议您自己百度"

    def actor_list(self, query):
        query = query.split(" ")
        result = self.tg.run("match(n:Person)-[:actedin]-(m:Movie) where m.title =$title return n.name",
                        title=query[0]).to_data_frame()
        word = ""
        for i in range(len(result)):
            word += result.iat[i, 0]+","
        if result.empty == False:
            return "该电影由" + word + "等主演"
        else:
            return "抱歉，建议您自己百度"

    def actor_introduction(self, query):
        query = query.split(" ")
        result = self.tg.run("match(n:Person) where n.name=$name return n.biography",
                        name=query[0]).to_data_frame()
        if result.empty == False and len(result.iat[0, 0]) >= 2:
            return result.iat[0, 0]
        else:
            return "抱歉，建议您自己百度"

    def actor_moive_gene(self, query):
        query = query.split(" ")
        result = self.tg.run("match(n:Person)-[:actedin]-(m:Movie) where n.name =$name "
                + "match(g:Genre)-[:is]-(m) where g.name=~$gname return m.title",
                        name=query[0],gname=query[1][0:-1]).to_data_frame()
        word = ""
        for i in range(len(result)):
            word += result.iat[i, 0] + ","
        if result.empty == False:
            return query[0] + "演过的" + query[1] + "有" + word
        else:
            return "抱歉，建议您自己百度"

    def actor_moive_total(self, query):
        query = query.split(" ")
        result = self.tg.run("match(n:Person)-[:actedin]->(m:Movie) where n.name=$name return m.title",name=query[0]).to_data_frame()
        word = ""
        for i in range(len(result)):
            word += result.iat[i, 0] + ","
        if result.empty == False:
            return query[0] + "一共出演过" + word + "等电影"
        else:
            return "抱歉，建议您自己百度"

    def score_more(self, query):
        query = query.split(" ")
        result = self.tg.run("match(n:Person)-[:actedin]-(m:Movie) where n.name =$name"
                        " and m.rating > $score return m.title", name=query[0], score=int(query[3])).to_data_frame()
        word = ""
        for i in range(len(result)):
            word += result.iat[i, 0] + ","
        if result.empty == False:
            return query[0] + "演过大于" + query[3] + "分的电影有" + word
        else:
            return "抱歉，建议您自己百度"

    def score_less(self, query):
        query = query.split(" ")
        print(query[3])
        result = self.tg.run("match(n:Person)-[:actedin]-(m:Movie) where n.name =$name and m.rating < $score return m.title",
                        name=query[0], score=int(query[3])).to_data_frame()
        word = ""
        for i in range(len(result)):
            word += result.iat[i, 0] + ","
        if result.empty == False:
            return query[0] + "演过小于" + query[3] + "分的电影有" + word
        else:
            return "抱歉，建议您自己百度"

    def actor_gene(self, query):
        query = query.split(" ")
        result = self.tg.run("match(n:Person)-[:actedin]-(m:Movie) where n.name = $name "
                + "match(p:Genre)-[:is]-(m) return distinct  p.name",name=query[0]).to_data_frame()
        word = ""
        for i in range(len(result)):
            word += result.iat[i, 0] + ","
        if result.empty == False:
            return query[0] + "演过" + word + "等类型的电影"
        else:
            return "抱歉，建议您自己百度"

    # todo
    def actor_a_b(self, query):
        query = query.split(" ")
        result = self.tg.run("match(n:Person)-[:actedin]-(m:Movie) where n.name =$nname "
                        "match(p:Person)-[:actedin]-(m) where p.name=$pname return distinct m.title"
                        , nname=query[0], pname=query[1]).to_data_frame()
        word = ""
        for i in range(len(result)):
            word += result.iat[i, 0] + ","
        if result.empty == False:
            return query[0] + "和" + query[1] + "合作演过" + word
        else:
            return "抱歉，建议您自己百度"

    def act_num(self, query):
        query = query.split(" ")
        result = self.tg.run("match(n)-[:actedin]-(m) where n.name =$name return count(*)", name=query[0]).to_data_frame()
        if result.empty == False:
            return "一共演了" + str(result.iat[0, 0]) + "部"
        else:
            return "抱歉，建议您自己百度"

    def birthday(self, query):
        query = query.split(" ")
        result = self.tg.run("match(n:Person) where n.name=$name return n.birth", name=query[0]).to_data_frame()
        if result.empty == False:
            return query[0] + "的生日是" + result.iat[0, 0]
        else:
            return "抱歉，建议您自己百度"


test_graph = Graph(
    "http://localhost:7474",
    username="neo4j",
    password="123"
)

# 单元测试入口
# if __name__ == "__main__":
#     sentence = "周润发出演过的喜剧片有哪些"
#     model = NaiveBayesModelMe(data_dir=data_dir, vocabularys=vocabularys)
#     # model.fit()
#     prediction = model.test(sentence)
#     query = match_question(prediction, sentence)
#
#     predict = predict(test_graph, query, prediction)
#     predict.run()
