

#
# Aggregation Queries
#

def qa1(db):
    '''
    Using $match and $group Stages
    '''

    return None
 

def qa2(db):
    '''
    Using $sort and $limit Stages
    '''

    return None
 

def qa3(db):
    '''
    Using $project, and $set Stages
    '''

    return None
 

def qa4(db):
    '''
    Using the $out Stage (and others)    
    '''

    return None
 
#
# Index Queries
#

def qi1(db):
    '''
    List indexes 
    '''
    
    return None


def _clean_explain(explain_result):
    '''
    Clean explain result
    '''
    
    return explain_result


def qi2(db):
    '''
    Explain query  
    '''
    
    result = None    

    return(_clean_explain(result))


def qi3(db):
    '''
    Create index
    '''

    return None


def qi4(db):
    '''
    Remove index
    '''

    return None


#
# Replica Set
# 

def rs_init(client):
    '''
    Initialize replica set
    '''

    return None


def _clean_status(status_result):
    '''
    Clean replica set status result
    '''
    
    return status_result


def rs_status(client):
    '''
    Get replica set status
    '''

    result = None

    return _clean_status(result)


def rs_stepdown(client):
    '''
    Step down primary node
    '''

    return None


#
# Transactions
# 

def tx_q1( db, session):
    '''
    Decrement field by one
    '''

    return None


def tx_q2( db, session):
    '''
    Increment field by one
    '''

    return None
