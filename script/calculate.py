import networkx as nx
import math

class Graph:
    def __init__(self, nodes, edges, cur_community):
        self.cur_community = cur_community
        self.nodes = nodes
        self.edges = edges
        self.nodes_number = len(nodes)
        self.edges_number = len(edges)
        self.G = nx.Graph()
        #print("Node number",self.nodes_number)
        ##print(self.nodes_number)
        #print("Edge number",self.edges_number)
        ##print(self.edges_number)

    def init_graph(self):
        self.G.add_nodes_from(self.nodes)
        self.G.add_edges_from(self.edges)

    def calculate_Q(self):
        Q_value = 0
        for key in self.cur_community.keys():
            print("community",key)
            c = self.cur_community[key]
            degrees = 0
            inter_edges = 0
            for u in c:
                u_neighbors = set(nx.all_neighbors(self.G, u))
                degrees += len(u_neighbors)
                print(u_neighbors)
                for nbr in u_neighbors:
                    if nbr in c:
                        inter_edges += 1
            inter_edges /= 2
            # degrees *= 2
            cq = (float(inter_edges) / self.edges_number) - (float(degrees) / (2 * self.edges_number))**2
            print("Degree",degrees)
            print("inter_edges",inter_edges)
            # #print("cq",cq)
            # #print("edge num",self.edges_number)
            # #print((inter_edges / self.edges_number))
            Q_value += cq
        return Q_value

node_number = 34
nodes = range(1,node_number,1)
#print(nodes)

edges_file = "./karate_edges.txt"
community_file = "./LPA_5_karate_community.txt"
#community_file = "./InfoMap_karate_community.txt"

# parse file
# Open file     
edges = []   
edgefileHandler = open(edges_file,  "r")
while  True:
    # Get next line from file
    line  =  edgefileHandler.readline().split()
    # If line is empty then end of file reached
    if not line :
        break
    edges.append((int(line[0]),int(line[1])))
    #print(line)  
edgefileHandler.close()
#print(len(edges))

community = {}
commufileHandler = open(community_file,"r")
while True:
    # Get next line from file
    line  =  commufileHandler.readline().split()
    # If line is empty then end of file reached
    if not line:
        break
    community[int(line[1])] = []
    ##print(line)  
commufileHandler.close()

commufileHandler = open(community_file,"r")
while True:
    # Get next line from file
    line  =  commufileHandler.readline().split()
    # If line is empty then end of file reached
    if not line:
        break
    community[int(line[1])].append(int(line[0]))
    #print(line)  
commufileHandler.close()


#print(community)

G = Graph(nodes,edges,community)
G.init_graph()
print("Modularity",G.calculate_Q())
