import { randNum, mapAttributTypeToAWSContentShorthand } from "./utils.js";

const supportedFunctions = [
  "attribute_not_exists",
  "attribute_exists",
  "begins_with",
];
const supportedComparators = ["=", "<>", ">", "=>", "<", "<="];
const supportedConditionExpressions = ["between", "in"];
const supportedAttributeTypes = [
  "string",
  "stringset",
  "bool",
  "list",
  "array",
  "map",
  "object",
  "int",
  "float",
  "number",
  "double",
  "numberset",
  "null",
];

export function generateFilterExpressions(filterConditions) {
  let filterExpressions = "";
  let _ExpressionAttributeNames = {};
  let _ExpressionAttributeValues = {};
  let expressionData = {};
  let _ExpressionData = {};

  try {
    validateFilterConditions(filterConditions);
  } catch (err) {
    console.log(err);
    return;
  }

  for (let filterCondition of filterConditions) {
    try {
      if (
        supportedFunctions.includes(filterCondition.operation.toLowerCase())
      ) {
        expressionData = generateFilterExpressionForFunctions(filterCondition);
      }
      if (
        supportedComparators.includes(filterCondition.operation.toLowerCase())
      ) {
        expressionData = generateFilterExpressionForComparators(filterCondition);
      }
      if (
        supportedConditionExpressions.includes(filterCondition.operation.toLowerCase())
      ) {
        expressionData = generateFilterExpressionForConditionExpressions(filterCondition);
      }
      filterExpressions = filterExpressions.concat(expressionData.filterExpressions);
      _ExpressionAttributeNames = {
        ..._ExpressionAttributeNames,
        ...expressionData.expressionAttributeNames,
      };
      if (typeof expressionData.expressionAttributeValues !== "undefined") {
        _ExpressionAttributeValues = {
          ..._ExpressionAttributeValues,
          ...expressionData.expressionAttributeValues,
        };
      }
    } catch (err) {
      console.log(err);
      return;
    }
  }
  if (filterExpressions !== "") {
    //removing the trailing " AND " from expression.
    filterExpressions = filterExpressions.slice(0, -5);
  }
  _ExpressionData = {
    "expressionAttributeNames": _ExpressionAttributeNames,
    "expressionAttributeValues": _ExpressionAttributeValues,
    "filterExpressions": filterExpressions
  }
  return _ExpressionData;
}

function validateFilterConditions(filterConditions) {
  try {
    for (let filterCondition of filterConditions) {
      validateFilterCondition(filterCondition);
    }
  } catch (err) {
    throw err;
  }
}

function validateFilterCondition(filterCondition) {
  let filterOperation = filterCondition.operation.toLowerCase();
  let supportedOperators = [...supportedComparators, ...supportedFunctions];
  if (!supportedOperators.includes(filterOperation)) {
    throw new TypeError(
      `Unknown operator. Current supported operators includes: ${supportedOperators}`
    );
  }
  if (supportedComparators.includes(filterOperation)) {
    try {
      validateComparator(filterCondition);
    } catch (err) {
      throw err;
    }
  }
  if (supportedFunctions.includes(filterOperation)) {
    try {
      validateFunction(filterCondition);
    } catch (err) {
      throw err;
    }
  }

  if (supportedConditionExpressions.includes(filterOperation)) {
    try {
      validateConditionExpression(filterCondition);
    } catch (err) {
      throw err;
    }
  }
}

function validateFunction(filterCondition) {
  switch (filterCondition.operation.toLowerCase()) {
    case "begins_with":
      if (
        typeof filterCondition.values !== "undefined" &&
        filterCondition.values !== ""
      ) {
        if (filterCondition.values[0] == "") {
          throw new TypeError(
            `Missing Value for attribute: ${filterCondition.attribute} and operator: ${filterCondition.operation}`
          );
        }
      } else if (
        typeof filterCondition.values == "undefined" ||
        filterCondition.values == ""
      ) {
        throw new TypeError(
          `Missing Value for attribute: ${filterCondition.attribute} and operator: ${filterCondition.operation}`
        );
      }
      if (typeof filterCondition.values[0] !== "String") {
        throw new TypeError(
          `Operand for operator: ${filterCondition.operation} must be of type String.`
        );
      }
      break;
    default:
      break;
  }
}

function validateComparator(filterCondition) {
  if (
    typeof filterCondition.values !== "undefined" &&
    filterCondition.values !== ""
  ) {
    if (filterCondition.values[0] == "") {
      throw new TypeError(
        `Missing Value for attribute: ${filterCondition.attribute} and operator: ${filterCondition.operation}`
      );
    }
  } else if (
    typeof filterCondition.values == "undefined" ||
    filterCondition.values == ""
  ) {
    throw new TypeError(
      `Missing Value for attribute: ${filterCondition.attribute} and operator: ${filterCondition.operation}`
    );
  }
  if (
    typeof filterCondition.type == "undefined" &&
    filterCondition.type == ""
  ) {
    throw new TypeError(
      `Missing type for attribute: ${filterCondition.attribute}`
    );
  } else {
    if (
      mapAttributTypeToAWSContentShorthand(filterCondition.type) ==
      "not_supported"
    ) {
      throw new TypeError(
        `Unsupported type. supported types are: ${supportedAttributeTypes}`
      );
    }
  }
}

function validateConditionExpression(filterCondition) {
  if (
    typeof filterCondition.values !== "undefined" &&
    filterCondition.values !== ""
  ) {
    if (filterCondition.values[0] == "") {
      throw new TypeError(
        `Missing Value for attribute: ${filterCondition.attribute} and operator: ${filterCondition.operation}`
      );
    }
  } else if (
    typeof filterCondition.values == "undefined" ||
    filterCondition.values == ""
  ) {
    throw new TypeError(
      `Missing Value for attribute: ${filterCondition.attribute} and operator: ${filterCondition.operation}`
    );
  }
  if (
    typeof filterCondition.type == "undefined" &&
    filterCondition.type == ""
  ) {
    throw new TypeError(
      `Missing type for attribute: ${filterCondition.attribute}`
    );
  } else {
    if (
      mapAttributTypeToAWSContentShorthand(filterCondition.type) ==
      "not_supported"
    ) {
      throw new TypeError(
        `Unsupported type. supported types are: ${supportedAttributeTypes}`
      );
    }
  }

  switch (filterOperation) {
    case "between":
      if (filterCondition.values.length !== 2) {
        throw new TypeError(
          `Condition: ${filterCondition.operation} must have two values`
        );
      }
      break;
    case "in":
      if (filterCondition.values.length < 1) {
        throw new TypeError(
          `Condition: ${filterCondition.operation} must have at least one value`
        );
      } else if (filterCondition.values.length > 100) {
        throw new TypeError(
          `Condition: ${filterCondition.operation} can only have a max of 100 values`
        );
      }
      break;
  }
}

function generateFilterExpressionForFunctions(filterCondition) {
  try {
    let filterExpressions = "";
    let _ExpressionAttributeValues = {};
    let _ExpressionAttributeNames = {};
    let expressionName = `#${filterCondition.attribute}`;
    let attributeName = `${filterCondition.attribute}`;
    _ExpressionAttributeNames[expressionName] = attributeName;

    switch (filterCondition.operation.toLowerCase()) {
      case "attribute_not_exists":
        filterExpressions = filterExpressions.concat(
          `attribute_not_exists(${expressionName}) AND `
        );
        break;
      case "attribute_exists":
        filterExpressions = filterExpressions.concat(`attribute_exists(${expressionName}) AND `);
        break;
      case "begins_with":
        let expressionValue = `:${randNum()}${
          filterCondition.attribute
        }${randNum}`;
        let attributeValue = `${filterCondition.values[0]}`;
        let attributeType = mapAttributTypeToAWSContentShorthand("string");
        _ExpressionAttributeValues[expressionValue] = {
          [attributeType]: attributeValue,
        };
        filterExpressions = filterExpressions.concat(
          `begins_with(${expressionName}, ${expressionValue}) AND `
        );
        break;
      default:
        break;
    }
    let expressionsData = {
      expressionAttributeNames: _ExpressionAttributeNames,
      expressionAttributeValues: _ExpressionAttributeValues,
      filterExpressions: filterExpressions,
    };
    return expressionsData;
  } catch (err) {
    console.log("generateFilterExpressionForFunctions failed");
    console.log(err);
    return;
  }
}

function generateFilterExpressionForComparators(filterCondition) {
  try {
    let filterExpressions = "";
    let _ExpressionAttributeNames = {};
    let _ExpressionAttributeValues = {};
    let _operation = filterCondition.operation;
    let expressionName = `#${filterCondition.attribute}`;
    let attributeName = `${filterCondition.attribute}`;
    _ExpressionAttributeNames[expressionName] = attributeName;
    let expressionValue = `:${randNum()}${filterCondition.attribute}`;
    let attributeValue = `${filterCondition.values[0]}`;
    let attributeType = mapAttributTypeToAWSContentShorthand(
      filterCondition.type
    );
    _ExpressionAttributeValues[expressionValue] = {
      [attributeType]: attributeValue,
    };
    filterExpressions = filterExpressions.concat(
      `${expressionName} ${_operation} ${expressionValue} AND `
    );

    let expressionsData = {
      expressionAttributeNames: _ExpressionAttributeNames,
      expressionAttributeValues: _ExpressionAttributeValues,
      filterExpressions: filterExpressions,
    };
    return expressionsData;
  } catch (err) {
    console.log("generateFilterExpressionForComparators failed");
    console.log(err);
    return;
  }
}

function generateFilterExpressionForConditionExpressions(filterCondition) {
  try {
    let filterExpressions = "";
    let _ExpressionAttributeNames = {};
    let _ExpressionAttributeValues = {};
    let _operation = filterCondition.operation.toLowerCase();
    let expressionName = `#${filterCondition.attribute}`;
    let attributeName = `${filterCondition.attribute}`;
    _ExpressionAttributeNames[expressionName] = attributeName;
    let expressionValue;
    let attributeValue;
    let attributeType;

    switch (filterCondition.operation.toLowerCase()) {
      case "between":
        expressionValue = `:${randNum()}${filterCondition.attribute}`;
        filterExpressions = filterExpressions.concat(
          `${expressionName} ${_operation.toUpperCase()} `
        );

        for (let val in filterCondition.values) {
            filterExpressions = filterExpressions.concat(`${expressionValue}${val} AND `);
          attributeValue = `${filterCondition.values[val]}`;
          attributeType = mapAttributTypeToAWSContentShorthand(
            filterCondition.type
          );
          _ExpressionAttributeValues[`${expressionValue}${val}`] = {
            [attributeType]: attributeValue,
          };
        }
        filterExpressions = filterExpressions.slice(0, -5);

        break;

      case "in":
        expressionValue = `:${randNum()}${filterCondition.attribute}`;
        filterExpressions = filterExpressions.concat(
          `${expressionName} ${_operation.toUpperCase()} `
        );
        let tempString = "";
        for (let val in filterCondition.values) {
          tempString.concat(`${expressionValue}${val}, `);
          attributeValue = `${filterCondition.values[val]}`;
          attributeType = mapAttributTypeToAWSContentShorthand(
            filterCondition.type
          );
          _ExpressionAttributeValues[`${expressionValue}${val}`] = {
            [attributeType]: attributeValue,
          };
        }
        tempString = tempString.slice(0, -2);
        filterExpressions = filterExpressions.concat(`(${tempString}) AND`);
        break;
      default:
        break;
    }

    let expressionsData = {
      expressionAttributeNames: _ExpressionAttributeNames,
      expressionAttributeValues: _ExpressionAttributeValues,
      filterExpressions: filterExpressions,
    };
    return expressionsData;
  } catch (err) {
    console.log("generateFilterExpressionForConditionExpressions failed");
    console.log(err);
    return;
  }
}

//https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html#Expressions.OperatorsAndFunctions.Syntax

//TODO: Validate all input at the begining of the lambda instead of wtihin each function.
