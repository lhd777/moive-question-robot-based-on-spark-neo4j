## moive-question-robot-based-on-spark-neo4j

### version1 moive-robot

we use the pyspark framework to bulid our robot.

**配置环境:** 

pyspark 2.4.4

py4j 0.10.7

Neo4j 1.2.3

**configuration environment:**

pyspark 2.4.4

py4j 0.10.7

Neo4j 1.2.3

### version2 moive-robot-spark-self

In this part, **we reproduce the spark by fully python**. So we do not need to have pyspark. But this spark by fully python is not distribute. It can only run on a single node. If you want to know that we how to build a distribute spark on our dfs(distribute file system), just git my another repository.

**configuration environment:**

Neo4j 1.2.3

## supplement

The model have already been trained. We have already upload the parameter. If you want to retrain the model, just check the code which show the train process.

## sample

- python client.py
- python server.py

Then you can get the UI to communicate with the robot.

![](ui.png)

## QA
If you have any question with my code, just connect with me.

email: [Lihuadong97@163.com](Lihuadong97@163.com)
