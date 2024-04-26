import { mapAttributTypeToAWSContentShorthand } from "./utils.js";

export function composePutExpressions(attributeList, tableName){
    let putItem = {};
    for (let attribute of attributeList) {
      try {
        let expressionName = `${Object.keys(attribute)[0]}`;
        let attributeValue = `${Object.values(attribute)[0].value}`; 
        let attributeType = mapAttributTypeToAWSContentShorthand(
          Object.values(attribute)[0].type
        );
        if (attributeType == "L") {
          if (attributeValue !== "") {
            putItem[expressionName] = {
              [attributeType]: [attributeValue],
            };
          } else {
            putItem[expressionName] = {
              [attributeType]: [],
            };
          }
        } else {
          putItem[expressionName] = {
            [attributeType]: attributeValue,
          };
        }
      } catch (err) {
        console.log(err);
        throw err;
      }
    }
  
  
    let putParams = {
      Put: {
        Item: putItem,
        TableName: tableName
      },
    };
  
    return putParams;
  }