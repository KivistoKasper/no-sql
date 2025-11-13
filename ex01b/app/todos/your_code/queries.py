

#
# Aggregation Queries
#

def qa1(db):
    '''
    Using $match and $group Stages
    '''
    result = db.courses.aggregate([
        {'$match': {'ects': {'$gt': 2}}},
        {'$group': {
            '_id': '$category',
            'courseCount': {'$count': {}}
            }}
    ])
    return result
 

def qa2(db):
    '''
    Using $sort and $limit Stages
    '''
    
    return db.courses.find(
        {},
    ).sort(
        {'ects': -1}
    ).limit(1)
 

def qa3(db):
    '''
    Using $project, and $set Stages
    '''
    result = db.courses.aggregate([
        {'$set': {
            'course': {
                '$concat': ['$_id', ' : ', '$name']
            }
        }},
        {'$project': {
            '_id': 0, 
            'course': 1
        }},
    ])

    return result
 

def qa4(db):
    '''
    Using the $out Stage (and others)    
    '''
    result = db.courses.aggregate([
        {'$group': {
            '_id': '$category',
            'courses': {'$count': {}}
        }},
        {'$sort': {'_id': 1}},
        {"$project": {
            "_id": 0, 
            "category": "$_id", 
            "courses": 1
        }},
        {'$out': 'categories'},
    ])



    return result
 
#
# Index Queries
#

def qi1(db):
    '''
    List indexes 
    '''
    
    return db.courses.index_information()


def _clean_explain(explain_result):
    '''
    Clean explain result
    '''
    #return explain_result
    return explain_result['queryPlanner']['winningPlan']


def qi2(db):
    '''
    Explain query  
    '''

    result = db.courses.find(
        {'category': 'B'}
    ).explain()
    
    
    """
    result = db.command({
        'explain': {
            'find': 'courses',
            'filter': {'category': 'B'}
        },
        'verbosity': 'allPlansExecution'
    })
    """
    return(_clean_explain(result))


def qi3(db):
    '''
    Create index
    '''

    return db.courses.create_index('category')


def qi4(db):
    '''
    Remove index
    '''

    return db.courses.drop_index('category_1')


#
# Replica Set
# 

def rs_init(client):
    '''
    Initialize replica set
    '''
    config = {'_id': 'rs', 'members': [
        {'_id': 0, 'host': 'mongo0'},
        {'_id': 1, 'host': 'mongo1'},
        {'_id': 2, 'host': 'mongo2'}]}

    return client.admin.command("replSetInitiate", config)


def _clean_status(status_result):
    '''
    Clean replica set status result
    '''
    return status_result['members']
    #return status_result


def rs_status(client):
    '''
    Get replica set status
    '''

    result = client.admin.command('replSetGetStatus')

    return _clean_status(result)


def rs_stepdown(client):
    '''
    Step down primary node
    '''

    return client.admin.command('replSetStepDown', 60)


#
# Transactions
# 

def tx_q1( db, session):
    '''
    Decrement field by one
    '''

    return db.courses.update_one(
        {'_id': 'DPHS.230'},
        {'$inc': {'ects': 1}},
        session=session
    )


def tx_q2( db, session):
    '''
    Increment field by one
    '''

    return db.courses.update_one(
        {'_id': 'KONE.411'},  
        {'$inc': {'ects': -1}},
        session=session
    )
