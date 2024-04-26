import { GetItemCommand, BatchGetItemCommand } from "@aws-sdk/client-dynamodb";
import { generateProjectionExpressions } from "./projectionExpressions.js";

  /*
  primaryKey
  {
    pk: "pk",
    pk_value: "UserBadge",
    sk: "sk",
    sk_value: "UserBadge#12312312"
  }
  */  


export async function getItem(
  full_tableName,
  primaryKey,
  queryConfig,
  dynamodbClient
) {
  let _ProjectionExpression = "";
  let _Key = {};
  let _primaryKey;
  let _primaryKeyValue;
  let _projectAllFields;
  let _sortKeyValue;

  let input = {
    TableName: full_tableName,
  };

  try {
    _primaryKey = primaryKey[0].pk;
    _primaryKeyValue = primaryKey[0].pk_value;
    _Key[_primaryKey] = {
        S: _primaryKeyValue,
      };
  } catch (err) {
    console.log(err);
    throw new Error(`Missing key value for getItem operation. Err: ${err}.`);
  }

  //using composit PrimaryKey
  if (typeof primaryKey[0].sk !== "undefined") {
    if (typeof primaryKey[0].sk_value!== "undefined") {
      _sortKeyValue = primaryKey[0].sk_value;
      _Key[primaryKey[0].sk] = {
        S: _sortKeyValue,
      };
    } else {
      throw new Error(
        `SortKey is provided. Missing sortkey value for getItem operation.`
      );
    }
  }

  input.Key = _Key;

  try {
    if (typeof queryConfig.projectAllFields !== "undefined") {
      if (typeof queryConfig.projectAllFields == "boolean") {
        _projectAllFields = queryConfig.projectAllFields;
        if (!_projectAllFields) {
          if (typeof queryConfig.projectionFields !== "undefined") {
            let projectionExpressionData = generateProjectionExpressions(
              queryConfig.projectionFields
            );
            input.ProjectionExpression =
              projectionExpressionData.projectionExpressions;
            if (
              Object.keys(projectionExpressionData.expressionAttributeNames)
                .length !== 0
            ) {
              input.ExpressionAttributeNames = {
                ...input.ExpressionAttributeNames,
                ...projectionExpressionData.expressionAttributeNames,
              };
            }
          } else {
            //default to use primary key as sole output.
            _ProjectionExpression = `#${_primaryKey}`;
            input.ProjectionExpression = _ProjectionExpression;
          }
        } else {
          //projectAllFields == true => Project all fields
        }
      } else {
        throw new TypeError(
          `Incorrect Type for ProjectAllFields: ${typeof queryConfig.projectAllFields}. ProjectAllFields must be of type boolean.`
        );
      }
    } else {
      //if queryConfig.projectAllFields == "undefined" => Project all fields
    }
  } catch (err) {
    console.log(err);
    throw err;
  }

  const command = new GetItemCommand(input);
  const response = await dynamodbClient.send(command);
  return response;
}

//TODO: update getItems to support composit Keys
export async function getItems(
  full_tableName,
  primaryKeys,
  queryConfig,
  dynamodbClient
) {
  let _ProjectionExpression = "";
  let _ExpressionAttributeNames = {};
  let responseList = [];
  let keys = [];
  let _batchGetItems = {};
  let batchcount = 0;
  let keyCount = 0;
  let _projectAllFields;

  let _primaryKey = primaryKeys[0].pk;
  let _sortKey = primaryKeys[0].sk;
  

  try {
    if (typeof queryConfig.projectAllFields !== "undefined") {
      if (typeof queryConfig.projectAllFields == "boolean") {
        _projectAllFields = queryConfig.projectAllFields;
        if (!_projectAllFields) {
          if (typeof queryConfig.projectionFields !== "undefined") {
            let projectionExpressionData = generateProjectionExpressions(
              queryConfig.projectionFields
            );
            _ProjectionExpression =
              projectionExpressionData.projectionExpressions;
            if (
              Object.keys(projectionExpressionData.expressionAttributeNames)
                .length !== 0
            ) {
              _ExpressionAttributeNames = {
                ..._ExpressionAttributeNames,
                ...projectionExpressionData.expressionAttributeNames,
              };
            }
          } else {
            //default to use primary key as sole output.
            _ProjectionExpression = `#${_primaryKey}`;
          }
        } else {
          //projectAllFields == true => Project all fields
        }
      } else {
        throw new TypeError(
          `Incorrect Type for ProjectAllFields: ${typeof queryConfig.projectAllFields}. ProjectAllFields must be of type boolean.`
        );
      }
    } else {
      //if queryConfig.projectAllFields == "undefined" => Project all fields
    }
  } catch (err) {
    console.log(err);
    throw err;
  }

  for (const primaryKey of primaryKeys) {
    keyCount += 1;
    let key = {};
    try{
      key[primaryKey.pk] = {
          S: primaryKey.pk_value,
      };
      if (typeof primaryKey.sk !== "undefined"){
        key[primaryKey.sk] = {
          S: primaryKey.sk_value,
      };
      }
    }catch (err){
      throw err
    }
    
    keys = [
      ...keys,
      key
    ];
    if (keyCount == (batchcount + 1) * 100) {
      batchcount += 1;
      try {
        _batchGetItems = {
          [full_tableName]: { Keys: keys },
        };

        if (Object.keys(_ExpressionAttributeNames).length > 0) {
          _batchGetItems[full_tableName] = {
            ..._batchGetItems[full_tableName],
            ExpressionAttributeNames: _ExpressionAttributeNames,
          };
        }
        if (_ProjectionExpression !== "") {
          _batchGetItems[full_tableName] = {
            ..._batchGetItems[full_tableName],
            ProjectionExpression: _ProjectionExpression,
          };
        }
        let response = await batchGetItems(_batchGetItems, dynamodbClient);
        if (typeof response["Responses"][full_tableName] !== "undefined") {
          responseList = [
            ...responseList,
            ...response["Responses"][full_tableName],
          ];
        }
      } catch (err) {
        console.log("batchGetItems failed during batch writing");
        throw new Error(
          `batchGetItems failed during batch writing. Err: ${err}`
        );
      }
      //if success
      _batchGetItems.length = 0;
    }
  }
  try {
    _batchGetItems = {
      [full_tableName]: { Keys: keys },
    };
    if (Object.keys(_ExpressionAttributeNames).length > 0) {
      _batchGetItems[full_tableName] = {
        ..._batchGetItems[full_tableName],
        ExpressionAttributeNames: _ExpressionAttributeNames,
      };
    }

    if (_ProjectionExpression !== "") {
      _batchGetItems[full_tableName] = {
        ..._batchGetItems[full_tableName],
        ProjectionExpression: _ProjectionExpression,
      };
    }
    //console.log("_batchGetItems");
    //console.log(JSON.stringify(_batchGetItems));
    let response = await batchGetItems(_batchGetItems, dynamodbClient);
    if (typeof response["Responses"][full_tableName] !== "undefined") {
      responseList = [
        ...responseList,
        ...response["Responses"][full_tableName],
      ];
    }
    return responseList;
  } catch (err) {
    console.log("batchGetItems failed during batch writing");
    throw new Error(`batchGetItems failed during batch writing. Err: ${err}`);
  }
}

async function batchGetItems(batchGetItems, dynamodbClient) {
  const batchGetParams = {
    RequestItems: batchGetItems,
  };
  const command = new BatchGetItemCommand(batchGetParams);
  const response = await dynamodbClient.send(command);
  return response;
}
