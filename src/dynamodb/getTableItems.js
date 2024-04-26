import { getItem, getItems } from "./getItem.js";
import { queryTable } from "./queryTable.js";
import { scanTable } from "./scanTable.js";

export async function getTableItems(full_tableName, primaryKeys = [], limit = -1, LastEvaluatedKey = "", queryConfig, dynamodbClient){
    let result;
    if (typeof queryConfig.mode !== "undefined"){
      switch(queryConfig.mode.toLowerCase()){
        case "scan":
          result =  await scanTable(full_tableName, primaryKeys, limit, LastEvaluatedKey, queryConfig, dynamodbClient);
          break;
        case "query":
          result =  await queryTable(full_tableName, limit, LastEvaluatedKey, queryConfig, dynamodbClient);
          break;
        case "get":
          result =  await getItem(full_tableName, primaryKeys, queryConfig, dynamodbClient);
          break;
        case "batch_get":
          result =  await getItems(full_tableName, primaryKeys, queryConfig, dynamodbClient);
          break;
        default:
          throw new TypeError(
            `unknown query mode ${queryConfig.mode}. Please double check your input.`
          );
      }
      return result;
    }else {
      throw new TypeError(
        "Missing query mode. this is required to determin how we will be retriving the items to update."
      );
    }
    
  }