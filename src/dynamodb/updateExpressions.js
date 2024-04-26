import { mapAttributTypeToAWSContentShorthand } from "./utils.js"

const supportedOperations= [
    "+",
    "-",
    "concat",
  ];
  

export function composeUpdateExpressions(attributeList, keyName, keyValue, sortKeyAttributes = "", tableName) {
    let _UpdateExpression = "set ";
    let _ExpressionAttributeNames = {};
    let _ExpressionAttributeValues = {};

    try{
        validateAttributeOperations(attributeList);
    }catch(err){
        throw err;
    }
    
    for (let attribute of attributeList) {
      try {
        let expressionName = `#${Object.keys(attribute)[0]}`;
        let attributeName = `${Object.keys(attribute)[0]}`;
        _ExpressionAttributeNames[expressionName] = attributeName;
        let expressionValue = `:${Object.keys(attribute)[0]}`;
        let attributeValue = `${Object.values(attribute)[0].value}`;
        let attributeOperation = typeof Object.values(attribute)[0].operation !== "undefined"? Object.values(attribute)[0].operation: "";
        let expression;
       
        if (attributeOperation !== ""){
            expression = composeExpressionWithOperations(attributeOperation, expressionName, expressionValue)
        }else {
            expression = `${expressionName} = ${expressionValue},`;
        }
      
        let attributeType = mapAttributTypeToAWSContentShorthand(
          Object.values(attribute)[0].type
        );
        if (attributeType == "L") {
          if (attributeValue !== "") {
            _ExpressionAttributeValues[expressionValue] = {
              [attributeType]: [attributeValue],
            };
          } else {
            _ExpressionAttributeValues[expressionValue] = {
              [attributeType]: [],
            };
          }
        } else {
          _ExpressionAttributeValues[expressionValue] = {
            [attributeType]: attributeValue,
          };
        }
        
        _UpdateExpression = _UpdateExpression.concat(expression);
      } catch (err) {
        console.log(err);
        throw err;
      }
    }
    if (_UpdateExpression !== ""){
      _UpdateExpression = _UpdateExpression.slice(0,-1);
    }

    //populate Update Key
    let key = {
      [keyName]: {
        S: keyValue,
      }
    }

    if (sortKeyAttributes !== ""){
      let attributeType = mapAttributTypeToAWSContentShorthand(sortKeyAttributes.type);
      key[sortKeyAttributes.keyName] = {
        [attributeType]: sortKeyAttributes.value
      }
    }
  
    let updateParams = {
      Update: {
        ExpressionAttributeNames: _ExpressionAttributeNames,
        ExpressionAttributeValues: _ExpressionAttributeValues,
        UpdateExpression: _UpdateExpression,
        TableName: tableName,
        Key: key,
        ReturnValuesOnConditionCheckFailure: "ALL_OLD",
      },
    };
  
    return updateParams;
  }

  function composeExpressionWithOperations(attributeOperation, expressionName, expressionValue){
    let expression;
    switch (attributeOperation){
        case "+":
        case "-":
            expression = `${expressionName} = ${expressionName} ${attributeOperation} ${expressionValue},`;
            break;
        case "concat":
            expression = `${expressionName} = ${expressionName}#${expressionValue},`;
            break;
        default:
            break;
    }
      return expression;
  }

  function validateAttributeOperations(attributeList){
    for (let attribute of attributeList){
        let attributItem = Object.values(attribute)[0];
        let attributeOperation = typeof attributItem.operation !== "undefined"? attributItem.operation: "";
        if (attributeOperation !== ""){
            if (!supportedOperations.includes(attributeOperation) ){
                throw new Error (`Unsupported Operations. Current supported Operations includes: ${supportedOperations}`)
            }else {
                switch (attributeOperation.toLowerCase()){
                    case "+":
                    case "-":
                        if (attributItem.type.toLowerCase() !== "int"){
                            throw new TypeError (`Incorrect Type for operation: ${attributeOperation}. Expecting type Int.`)
                        }
                        break;
                    case "concat":
                        if (attributItem.type.toLowerCase() !== "string"){
                            throw new TypeError (`Incorrect Type for operation: ${attributeOperation}. Expecting type String.`)
                        }
                        break;
                    default:
                        break;
                }
            }
        }
        
    }
  }