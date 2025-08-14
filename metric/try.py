from metric.utils.mongosh_exec import MongoShellExecutor

executor = MongoShellExecutor(
    connection_string="mongodb://localhost:27017",
    mongosh_path="/opt/homebrew/bin/mongosh"  # <- your path
)

db = "school_bus"
mql = "db.school.aggregate([\n  {\n    $lookup: {\n      from: \"driver\",\n      localField: \"School_ID\",\n      foreignField: \"school_bus.School_ID\",\n      as: \"Docs1\"\n    }\n  },\n  {\n    $unwind: \"$Docs1\"\n  },\n  {\n    $project: {\n      \"School\": 1,\n      \"Name\": \"$Docs1.Name\",\n      \"_id\": 0\n    }\n  }\n]);\n"

print(executor.execute_query(db, mql))
