import * as fs from "fs";

const BATCH_SIZE = 100;

let connection = "YOUR_IP_ADDRESS";
let database = "YOUR_DATABASE_NAME";

//idPolicy: overwrite_with_same_id|always_insert_with_new_id|insert_with_new_id_if_id_exists|skip_documents_with_existing_id|abort_if_id_already_exists
let toImportContents = [
    { content: fs.readFileSync("/YOUR_PATH/locations.json", "utf8"), collection: "locations", idPolicy: "overwrite_with_same_id" }
];

let totalImportResult = {
    result: {},
    fails: [],
}

function importContent({content, collection, idPolicy}) {
    let offset = 0;
    let continueRead = true;

    totalImportResult.result[collection] = {
        nInserted: 0,
        nModified: 0,
        nSkipped: 0,
        failed: 0,
    };

    let collectionRst = totalImportResult.result[collection];

    console.log(`import docs to ${connection}:${database}:${collection} start...`);

    let srcDocs = mb.parseBSON(content);

    console.log(`Read ${srcDocs.length} Docs from content`);

    while (continueRead) {
        let docs = srcDocs.slice(offset, offset + BATCH_SIZE);
        let readLength = docs.length;

        offset = offset + readLength;
        if (readLength < BATCH_SIZE)
            continueRead = false;

        if (readLength) {
            let writeResult = mb.writeToDb({ connection, db: database, collection, docs, idPolicy });
            let failed = writeResult.errors.length;
            let success = writeResult.nInserted + writeResult.nModified;

            collectionRst.nInserted += writeResult.nInserted;
            collectionRst.nModified += writeResult.nModified;
            collectionRst.nSkipped += writeResult.nSkipped;
            collectionRst.failed += failed;

            console.log(`${collection}: ${collectionRst.nInserted + collectionRst.nModified} docs successfully imported, ${collectionRst.failed} docs failed.`);
            if (failed) {
                console.log("Failed objects", writeResult.errors);
            }

            totalImportResult.fails = [...totalImportResult.fails, ...writeResult.errors];
        }

        sleep(10)
    }

    console.log(`import docs to ${connection}:${database}:${collection} finished.`);
}

toImportContents.forEach(it => importContent(it));

if (totalImportResult.result) {
    let successed = 0;
    let failed = 0;
    let collections = _.keys(totalImportResult.result);
    collections.forEach((key) => {
        let obj = totalImportResult.result[key];
        successed += obj.nInserted + obj.nModified;
        failed += obj.failed;
    });
    console.log(`${successed} document(s) have been imported into ${collections.length} collection(s).`, totalImportResult.result);
    if (failed) {
        console.log(`${failed} document(s) haven't been imported, please check failed list below.`);
    } else {
        console.log("All documents imported successfully.");
    }
}

if (totalImportResult.fails.length) {
    console.log("All failed objects", totalImportResult.fails);
}

