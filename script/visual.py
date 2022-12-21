import networkx as nx
import matplotlib.pyplot as plt
import pandas as pd

edges = []
with open(r"/home/sfwu22/workspace/spark/spark-3.3.0-bin-hadoop3/lpa_project/data/karate/karate.txt", "r") as f:
    all_data = f.readlines()
    for lines in all_data: 
        lines = lines.split()
        lines[0] = int(lines[0])
        lines[1] = int(lines[1])
        edges.append(lines)
print(edges)
result = [
[22  , 2],
[14  , 2],
[8  , 2],
[12  , 2],
[18  , 2],
[20  , 2],
[13  , 2],
[11  , 2],
[7  , 2],
[5  , 2],
[34  , 23],
[4  , 32],
[16  , 32],
[28  , 32],
[30  , 32],
[32  , 32],
[24  , 32],
[6  , 32],
[26  , 32],
[10  , 32],
[2  , 32],
[19  , 32],
[15  , 32],
[21  , 32],
[25  , 32],
[29  , 32],
[27  , 32],
[33  , 32],
[23  , 32],
[1  , 32],
[17  , 32],
[3  , 32],
[9  , 32],
[31, 32]
]

G = nx.Graph()
G.add_edges_from(edges)


ID = []
group = []
for item in result:
    ID.append(item[0])
    group.append(item[1])


carac = pd.DataFrame({ 'ID':ID, 'group':group })
carac= carac.set_index('ID')
# 根据节点顺序设定值
carac=carac.reindex(G.nodes())
 
# And I need to transform my categorical column in a numerical value: group1->1, group2->2...
# 设定类别
carac['group']=pd.Categorical(carac['group'])
carac['group'].cat.codes


pos = nx.spring_layout(G)

plt.switch_backend('agg') # avoid error using ssh
nx.draw(G, pos, with_labels=True,node_color=carac['group'].cat.codes, cmap=plt.cm.Set1)
plt.savefig("./test.png")
