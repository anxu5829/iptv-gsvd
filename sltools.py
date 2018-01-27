"""
saving the function using for save and load data

"""

import numpy as np
import pandas as pd
from  scipy.sparse import csc_matrix
import pickle




def sparseToPandas(sparseMatrix):
    coo = sparseMatrix.tocoo(copy = False)
    data = pd.DataFrame(
        {'row': coo.row,
         'col': coo.col,
         'data': coo.data}
        )[['row', 'col', 'data']].\
        reset_index(drop=True)
    # sort_values(['row', 'col']).\
    return(data)


def pandasToSparse(pandasMatrix):
    csc = csc_matrix((pandasMatrix.data,
                     (pandasMatrix.row,pandasMatrix.col)))
    return csc


# save sparse matrix using numpu method
def save_sparse_csc(filename,array):
    np.savez(filename,data = array.data ,indices=array.indices,
             indptr =array.indptr, shape=array.shape )

# load sparse matrix
def load_sparse_csc(filename):
    loader = np.load(filename)
    return csc_matrix((  loader['data'], loader['indices'], loader['indptr']),
                         shape = loader['shape'])




# save / load data using cPickle


def save_pickle(Object, filename):
    with open(filename, 'wb') as outfile:
        pickle.dump(Object, outfile, pickle.HIGHEST_PROTOCOL)
def load_pickle(filename):
    with open(filename, 'rb') as infile:
        Object = pickle.load(infile)
    return Object



def loadDictInversed(filename):
    with open(filename, 'rb') as infile:
        Object = pickle.load(infile)
    if type(Object) == dict:
        return invertDict(Object)
    else:
        print("data is not dict !!")
        assert  Exception




def invertDict(my_map):
    inv_map = {v: k for k, v in my_map.items()}
    return  inv_map


# # these method is aborted
# def mergeDataToSparse(workfilename,numOfFile):
#     # workfilename = "D:\\tempdata\\"
#     list = []
#     for i in range(numOfFile):
#         file = pd.read_csv(workfilename+"dot_cosine"+str(i) +".gzip" ,compression= "gzip")
#         list.append(file)
#         del file
#
#     dot_cosine = pd.concat(list)
#     dot_cosine = pandasToSparse(dot_cosine)
#     return dot_cosine
#
