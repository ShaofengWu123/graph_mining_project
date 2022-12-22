import networkx as nx
import matplotlib.pyplot as plt
import pandas as pd

edges = []
with open(r"./karate_edges.txt", "r") as f:
    all_data = f.readlines()
    for lines in all_data: 
        lines = lines.split()
        lines[0] = int(lines[0])
        lines[1] = int(lines[1])
        edges.append(lines)
print(edges)

result = []
# parse result
with open(r"./xxx.txt", "r") as f:
    all_data = f.readlines()
    for lines in all_data: 
        lines = lines.split()
        lines[0] = int(lines[0])
        lines[1] = int(lines[1])
        result.append(lines)
    

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
