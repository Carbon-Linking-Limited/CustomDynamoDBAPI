
import { ScanCommand } from "@aws-sdk/client-dynamodb";
import { generateFilterExpressions } from "./filterExpression.js";
import { generateProjectionExpressions } from "./projectionExpressions.js";

export async function scanTable(full_tableName, primaryKey,limit = -1, LastEvaluatedKey = "", queryConfig, dynamodbClient){
    let _ProjectionExpression = "";
    let _ExpressionAttributeNames = {};
    let _Limt;
    let _projectAllFields;

   
    const input = {
      "TableName": full_tableName,
    };
    
      try{
        if (typeof queryConfig.projectAllFields !== "undefined"){
            if(typeof queryConfig.projectAllFields == "boolean"){
                _projectAllFields =  queryConfig.projectAllFields;
                if (!_projectAllFields){
                    if (typeof queryConfig.projectionFields !== "undefined"){
                        let projectionExpressionData = generateProjectionExpressions(queryConfig.projectionFields); 
                        input.ProjectionExpression = projectionExpressionData.projectionExpressions;
                        if (Object.keys(projectionExpressionData.expressionAttributeNames).length !== 0){
                            input.ExpressionAttributeNames = {...input.ExpressionAttributeNames, ...projectionExpressionData.expressionAttributeNames}
                        }
                    } else {
                        //default to use primary key as sole output.
                        _ProjectionExpression = `#${primaryKey}`;
                        input.ProjectionExpression = _ProjectionExpression;
                    }
                } else {
                    //projectAllFields == true => Project all fields
                }
            }else{
                throw new TypeError(`Incorrect Type for ProjectAllFields: ${typeof queryConfig.projectAllFields}. ProjectAllFields must be of type boolean.`);
            }
        } else{
            //if queryConfig.projectAllFields == "undefined" => Project all fields
        }
       
      }catch(err){
        console.log(err);
        throw err;
      }

      try{
        if (typeof queryConfig.filters !== "undefined"){
            let filterExpressionData = generateFilterExpressions(queryConfig.filters); 
            input.FilterExpression = filterExpressionData.filterExpressions;
            if (Object.keys(filterExpressionData.expressionAttributeNames).length !== 0){
                input.ExpressionAttributeNames = {...input.ExpressionAttributeNames, ...filterExpressionData.expressionAttributeNames}
            }
            if(Object.keys(filterExpressionData.expressionAttributeValues).length !== 0){
                input.ExpressionAttributeValues = {...input.ExpressionAttributeValues, ...filterExpressionData.expressionAttributeValues}
            }
          }
      }catch(err){
        console.log(err);
        throw err;
      }
    
      if (LastEvaluatedKey !== ""){
        input.ExclusiveStartKey = LastEvaluatedKey;
      }

    if (limit > 0){
      _Limt = limit;
      input.Limit = _Limt;
    }

    if(typeof input.ProjectionExpression == "undefined" && typeof input.FilterExpression == "undefined" &&  input.ExpressionAttributeValues == "undefined"){
      // no expressions
  }else {
      try {
          let expressionName = `#${primaryKey}`;
          let attributeName = `${primaryKey}`;
          _ExpressionAttributeNames[expressionName] = attributeName;
          input.ExpressionAttributeNames = _ExpressionAttributeNames;
        } catch (err) {
          console.log(err);
        }
  }
    const command = new ScanCommand(input);
    const response = await dynamodbClient.send(command);
  
    //rsponse.LastEvaluatedKey
    return response;
  }