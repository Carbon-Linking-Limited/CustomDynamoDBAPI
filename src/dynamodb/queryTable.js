import { QueryCommand } from "@aws-sdk/client-dynamodb";
import { mapAttributTypeToAWSContentShorthand } from "./utils.js";
import { generateFilterExpressions } from "./filterExpression.js";
import { generateProjectionExpressions } from "./projectionExpressions.js";

//TODO: work on supporting all conditions
const supportedConditionExpressions = ["begins_with"];

export async function queryTable(
  full_tableName,
  limit = -1,
  LastEvaluatedKey = "",
  queryConfig,
  dynamodbClient
) {
  let _ProjectionExpression = "";
  let _ExpressionAttributeNames = {};
  let _ExpressionAttributeValues = {};
  let _KeyConditionExpression = "";
  let _Limt;
  let _IndexName;
  let _PartitionKeyName;
  let _PartitionKeyValue;
  let _SortKeyName;
  let _SortKeyValue;
  let _SortKeyType;
  let _projectAllFields;
  let _SortDirection;
  let _ConditionExpressions;

  try {
    validateQueryConfigs(queryConfig);
    _IndexName = typeof queryConfig.indexName !== "undefined"? queryConfig.indexName: "";
    _PartitionKeyName = queryConfig.partitionKeyName;
    _PartitionKeyValue = queryConfig.partitionKeyValue;
    _SortKeyName =
      typeof queryConfig.sortKeyName !== "undefined"
        ? queryConfig.sortKeyName
        : "";
    _SortKeyValue =
      typeof queryConfig.sortKeyValue !== "undefined"
        ? queryConfig.sortKeyValue
        : "";
    _SortKeyType =
      typeof queryConfig.sortKeyType !== "undefined"
        ? queryConfig.sortKeyType
        : "";
    _SortDirection =
      typeof queryConfig.sortDirection !== "undefined"
        ? queryConfig.sortDirection
        : "";
    _ConditionExpressions =
      typeof queryConfig.conditionExpressions !== "undefined"
        ? queryConfig.conditionExpressions
        : "";
  } catch (err) {
    console.log(err);
    return err;
  }

  const input = {
    TableName: full_tableName,
  };

  try {
    let expressionName = `#${_PartitionKeyName}`;
    let attributeName = `${_PartitionKeyName}`;
    _ExpressionAttributeNames[expressionName] = attributeName;
    let expressionValue = `:${_PartitionKeyName}`;
    let attributeValue = `${_PartitionKeyValue}`;
    let attributeType = mapAttributTypeToAWSContentShorthand("String");
    _ExpressionAttributeValues[expressionValue] = {
      [attributeType]: attributeValue,
    };
    _KeyConditionExpression = `${expressionName} = ${expressionValue}`;
    if (_SortKeyName !== "" && _SortKeyValue !== "" && _SortKeyType !== "") {
      expressionName = `#${_SortKeyName}`;
      attributeName = `${_SortKeyName}`;
      _ExpressionAttributeNames[expressionName] = attributeName;
      expressionValue = `:${_SortKeyName}`;
      attributeValue = `${_SortKeyValue}`;
      attributeType = mapAttributTypeToAWSContentShorthand(_SortKeyType);
      _ExpressionAttributeValues[expressionValue] = {
        [attributeType]: attributeValue,
      };
      if (_ConditionExpressions !== "") {
        switch (_ConditionExpressions) {
          case "begins_with":
            _KeyConditionExpression = _KeyConditionExpression.concat(
              ` AND begins_with(${expressionName}, ${expressionValue})`
            );
            break;
          default:
            console.log(
              `unsupported condition expressions: ${_ConditionExpressions}`
            );
            throw new Error(
              `unsupported condition expressions: ${_ConditionExpressions}`
            );
        }
      } else {
        _KeyConditionExpression = _KeyConditionExpression.concat(
          ` AND ${expressionName} = ${expressionValue}`
        );
      }
    }
    if (_IndexName !== ""){
      input.IndexName = _IndexName;
    }
    input.ExpressionAttributeValues = _ExpressionAttributeValues;
    input.ExpressionAttributeNames = _ExpressionAttributeNames;
    input.KeyConditionExpression = _KeyConditionExpression;
  } catch (err) {
    console.log(err);
  }

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
            _ProjectionExpression = `#${_PartitionKeyName}`;
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

  try {
    if (typeof queryConfig.filters !== "undefined") {
      let filterExpressionData = generateFilterExpressions(queryConfig.filters);
      input.FilterExpression = filterExpressionData.filterExpressions;
      if (
        Object.keys(filterExpressionData.expressionAttributeNames).length !== 0
      ) {
        input.ExpressionAttributeNames = {
          ...input.ExpressionAttributeNames,
          ...filterExpressionData.expressionAttributeNames,
        };
      }
      if (
        Object.keys(filterExpressionData.expressionAttributeValues).length !== 0
      ) {
        input.ExpressionAttributeValues = {
          ...input.ExpressionAttributeValues,
          ...filterExpressionData.expressionAttributeValues,
        };
      }
    }
  } catch (err) {
    console.log(err);
    throw err;
  }
  if (LastEvaluatedKey !== "") {
    input.ExclusiveStartKey = JSON.parse(LastEvaluatedKey);
  }

  if (_SortDirection !== "") {
    switch (_SortDirection.toLowerCase()) {
      case "asc":
        input.ScanIndexForward = true;
        break;
      case "desc":
        input.ScanIndexForward = false;
        break;
      default:
        input.ScanIndexForward = true;
        break;
    }
  }

  if (limit > 0) {
    _Limt = limit;
    input.Limit = _Limt;
  }

  const command = new QueryCommand(input);
  const response = await dynamodbClient.send(command);

  //rsponse.LastEvaluatedKey
  return response;
}

function validateQueryConfigs(queryConfig) {

  if (
    typeof queryConfig.partitionKeyName == "undefined" ||
    queryConfig.partitionKeyName == ""
  ) {
    throw new TypeError(
      `Missing partitionKeyName. Please double check your input.`
    );
  }
  if (
    typeof queryConfig.partitionKeyValue == "undefined" ||
    queryConfig.partitionKeyValue == ""
  ) {
    throw new TypeError(
      `Missing partitionKeyValue. Please double check your input.`
    );
  }
  if (
    typeof queryConfig.sortKeyName !== "undefined" &&
    queryConfig.sortKeyName !== ""
  ) {
    if (
      typeof queryConfig.sortKeyValue == "undefined" ||
      queryConfig.sortKeyValue == ""
    ) {
      throw new TypeError(
        `sortKeyName is supplied without sortKeyValue or sortKeyType. Please input sortKeyValue and sortKeyType or remove sortKeyName.`
      );
    } else if (
      typeof queryConfig.sortKeyType == "undefined" ||
      queryConfig.sortKeyType == ""
    ) {
      throw new TypeError(
        `sortKeyName is supplied without sortKeyValue or sortKeyType. Please input sortKeyValue and sortKeyType or remove sortKeyName.`
      );
    }
  }
  if (
    typeof queryConfig.sortKeyValue !== "undefined" &&
    queryConfig.sortKeyValue !== ""
  ) {
    if (
      typeof queryConfig.sortKeyName == "undefined" ||
      queryConfig.sortKeyName == ""
    ) {
      throw new TypeError(
        `sortKeyValue is supplied without sortKeyName or sortKeyType. Please input sortKeyName and sortKeyType or remove sortKeyValue.`
      );
    } else if (
      typeof queryConfig.sortKeyType == "undefined" ||
      queryConfig.sortKeyType == ""
    ) {
      throw new TypeError(
        `sortKeyValue is supplied without sortKeyName or sortKeyType. Please input sortKeyName and sortKeyType or remove sortKeyValue.`
      );
    }
  }

  if (
    typeof queryConfig.sortKeyType !== "undefined" &&
    queryConfig.sortKeyType !== ""
  ) {
    if (
      typeof queryConfig.sortKeyName == "undefined" ||
      queryConfig.sortKeyName == ""
    ) {
      throw new TypeError(
        `sortKeyType is supplied without sortKeyName or sortKeyValue. Please input sortKeyName and sortKeyValue or remove sortKeyType.`
      );
    } else if (
      typeof queryConfig.sortKeyValue == "undefined" ||
      queryConfig.sortKeyValue == ""
    ) {
      throw new TypeError(
        `sortKeyType is supplied without sortKeyName or sortKeyType. Please input sortKeyName and sortKeyValue or remove sortKeyType.`
      );
    }
  }

  if (
    typeof queryConfig.sortDirection !== "undefined" &&
    queryConfig.sortDirection !== ""
  ) {
    if (
      queryConfig.sortDirection.toLowerCase() !== "asc" &&
      queryConfig.sortDirection.toLowerCase() !== "desc"
    ) {
      throw new TypeError(`sortDirection must either be "asc" or "desc".`);
    }
  }

  if (
    typeof queryConfig.conditionExpressions !== "undefined" &&
    queryConfig.conditionExpressions !== ""
  ) {
    if (
      typeof queryConfig.sortKeyName == "undefined" ||
      queryConfig.sortKeyName == ""
    ) {
      throw new TypeError(
        `No Sortkey provided. sortKey must be included to use condition expression.`
      );
    }
    if (
      supportedConditionExpressions.indexOf(
        queryConfig.conditionExpressions.toLowerCase()
      ) == -1
    ) {
      throw new TypeError(
        `unsupported conditionExpressions. current supported condition expressions are: ${supportedConditionExpressions}`
      );
    }
  }
}
