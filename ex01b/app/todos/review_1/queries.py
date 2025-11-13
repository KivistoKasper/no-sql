from app.supp.helpers import SomeError
from pprint import pprint
#from pymongo.errors import NotPrimaryError

#
# Aggregation Queries
#

def qa1(db):
    """
    Using $match and $group Stages
    """
    pipeline = [
        {"$match": {"ects": {"$gt": 2}}},
        {"$group": {"_id": "$category", "courseCount": {"$sum": 1}}}
    ]
    result = db.courses.aggregate(pipeline)
    for doc in result:
        pprint(doc)


def qa2(db):
    """
    Using $sort and $limit Stages
    """
    pipeline = [
        {"$sort": {"ects": -1}},
        {"$limit": 1}
    ]
    result = db.courses.aggregate(pipeline)
    for doc in result:
        pprint(doc)



def qa3(db):
    """
    Using $project, and $set Stages
    """
    pipeline = [
        {"$set": {"course": {"$concat": ["$_id", " : ", "$name"]}}},
        {"$project": {"course": 1, "_id": 0}}
    ]
    result = db.courses.aggregate(pipeline)
    for doc in result:
        pprint(doc)

def qa4(db):
    """
    Using the $out Stage (and others)
    """
    pipeline = [
        {"$group": {"_id": "$category", "category": {"$first": "$category"}, "courses": {"$sum": 1}}},
        {"$project": {"_id": 0, "category": 1, "courses": 1}},
        {"$out": "categories"}
    ]
    try:
        result = db.courses.aggregate(pipeline)
        for doc in result:
            pprint(doc)
    except Exception as e:
        raise SomeError(f"Error executing qa4 pipeline: {e}")


#
# Index Queries
#

def qi1(db):
    """
    List indexes
    """
    indexes = db.courses.index_information()
    for name, info in indexes.items():
        # Create a new dictionary with keys in the desired order
        ordered_info = {'key': info['key'], 'v': info['v']}
        print({name: ordered_info})

def _clean_explain(explain_result):
    """
    Clean explain result to match what we'd get by doing:
        pprint(result["queryPlanner"]["winningPlan"])
    """
    return explain_result["queryPlanner"]["winningPlan"]


def qi2(db):
    """
    Explain query at the default 'queryPlanner' level, then clean the result,
    so it only shows the 'winningPlan' portion.
    """
    query = {"category": "B"}
    try:
        result = db.courses.find(query).explain()  # No 'verbosity' argument
        cleaned = _clean_explain(result)  # Pass raw result to cleaner
        pprint(cleaned)  # Print only the 'winningPlan' portion
    except Exception as e:
        raise SomeError(f"Failed to explain query: {e}")


def qi3(db):
    """
    Create index (on category field)
    """
    index_name = db.courses.create_index([("category", 1)])
    print(f"Created index: {index_name}")



def qi4(db):
    """
    Drop index on category field
    """
    db.courses.drop_index("category_1")



#
# Replica Set
#


def rs_init(db_client):
    config = {
        '_id': 'myReplicaSet',
        'members': [
            {'_id': 0, 'host': 'localhost:27018'},
            {'_id': 1, 'host': 'localhost:27019'},
            {'_id': 2, 'host': 'localhost:27020'}
        ]
    }
    try:
        status = db_client.admin.command('replSetInitiate', config)
        print({'ok': status['ok']})
    except Exception as e:
        print(f"Error initializing replica set: {e}")


def rs_status(db_client):
    """
    Returns the status of the replica set.
    """
    try:
        status = db_client.admin.command('replSetGetStatus')
        return _clean_status(status)
    except Exception as e:
        if "no replset config has been received" in str(e).lower():
            return {"ok": 0, "msg": "No replica set configuration has been received."}
        raise Exception(f"Failed to retrieve replica set status: {e}")



def rs_stepdown(client):
    """
    Step down primary node.
    """
    try:
        status = client.admin.command({"replSetStepDown": 60, "force": True})
        print({'ok': status['ok']})
    
    except Exception as e:
        raise Exception(f"Error stepping down primary node: {e}")


def _clean_status(status_result):
    """
    Clean replica set status result.
    """
    return [
        {"_id": member["_id"], "name": member["name"], "stateStr": member["stateStr"]}
        for member in status_result.get("members", [])
    ]


#
# Transactions
#

def tx_q1(db, session):
    '''
    Decrement field by one
    '''
    try:
        return db.courses.update_one(
            {"_id": "KONE.411"},
            {"$inc": {"ects": -1}},
            session=session
        )
    except Exception as e:
        raise SomeError(f"Error in tx_q1: {e}")


def tx_q2(db, session):
    '''
    Increment field by one
    '''
    try:
        return db.courses.update_one(
            {"_id": "DPHS.230"},
            {"$inc": {"ects": 1}},
            session=session
        )
    except Exception as e:
        raise SomeError(f"Error in tx_q2: {e}")
