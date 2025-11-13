

#
# Aggregation Queries
#

def qa1(db):
    '''
    Using $match and $group Stages
    '''

    pipeline = [
        { 
            "$match": {
                "ects": { "$gt": 2 }
            }
        },
        { 
            "$group": {
                "_id": "$category",
                "courseCount": { "$sum": 1 }
            }
        }
    ]

    result = db.courses.aggregate(pipeline)

    return result
 

def qa2(db):
    '''
    Using $sort and $limit Stages
    '''

    pipeline = [
        {
            "$sort": {
                "ects": -1
            }
        },
        {
            "$limit": 1
        }
    ]

    result = db.courses.aggregate(pipeline)

    return result
 

def qa3(db):
    '''
    Using $project, and $set Stages
    '''

    pipeline = [
        {
            "$set": {
                "course": {
                    "$concat": [
                        "$_id",
                        " : ",
                        "$name"
                    ]
                }
            }
        },
        {
            "$project": {
                "course": 1,
                "_id": 0
            }
        }
    ]

    result = db.courses.aggregate(pipeline)

    return result
 

def qa4(db):
    '''
    Using the $out Stage (and others)    
    '''

    pipeline = [
        {
            "$sort": {
                "category": 1
            }
        },
        {
            "$group": {
                "_id": "$category",
                "courses": { "$sum": 1 }
            }
        },
        {
            "$project": {
                "category": "$_id",
                "courses": 1,
                "_id": 0
            }
        },
        {
            "$out": "categories"
        }
    ]

    result = db.courses.aggregate(pipeline)

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
    cleaned_result = explain_result['queryPlanner']['winningPlan']
    
    return cleaned_result


def qi2(db):
    '''
    Explain query  
    '''

    query = {'category': {'$eq': 'B'}}
    
    result = db.courses.find(query).explain()

    return(_clean_explain(result))


def qi3(db):
    '''
    Create index
    '''

    result = db.courses.create_index([("category", 1)])

    return result


def qi4(db):
    '''
    Remove index
    '''

    db.courses.drop_index("category_1")

    return None


#
# Replica Set
# 

def rs_init(client):
    '''
    Initialize replica set
    '''

    config = {
        "_id": "rs",
        "members": [
            {"_id": 0, "host": "mongo0:27017"},
            {"_id": 1, "host": "mongo1:27017"},
            {"_id": 2, "host": "mongo2:27017"}
        ]
    }

    result = client.admin.command("replSetInitiate", config)

    return result


def _clean_status(status_result):
    '''
    Clean replica set status result
    '''

    cleanded_status = status_result["members"]
    
    return cleanded_status


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

    result = client.admin.command('replSetStepDown', 60)

    return result


#
# Transactions
# 

def tx_q1( db, session):
    '''
    Decrement field by one
    '''

    result = db.courses.update_one(
        { "_id": "KONE.411" },
        { "$inc": { "ects": -1 }},
        session=session
    )

    return result


def tx_q2( db, session):
    '''
    Increment field by one
    '''

    result = db.courses.update_one(
        { "_id": "DPHS.230" },
        { "$inc": { "ects": 1 }},
        session=session
    )

    return result
