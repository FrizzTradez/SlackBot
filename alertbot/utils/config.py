# File For Storing External Data

es_impvol = None
nq_impvol = None
rty_impvol = None
cl_impvol = None
es_bias = None
nq_bias = None
rty_bias = None
cl_bias = None

def set_impvol(es, nq, rty, cl):
    global es_impvol, nq_impvol, rty_impvol, cl_impvol
    es_impvol = es
    nq_impvol = nq
    rty_impvol = rty
    cl_impvol = cl
    
def set_bias(es, nq, rty, cl):
    global es_bias, nq_bias, rty_bias, cl_bias
    es_bias = es
    nq_bias = nq
    rty_bias = rty
    cl_bias = cl