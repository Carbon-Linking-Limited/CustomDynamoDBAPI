import { batchWriteItems } from "./batchWriteItems.js";
import { writeItem } from "./writeItem.js";


export async function putTableItems(full_tableName, mode, dynamodbClient, writeItemList =[], writeItem = {}){
    let result;
      switch(mode){
        case "put":
          result =  await writeItem(writeItem, full_tableName, dynamodbClient);
          break;
        case "batch_put":
          result =  await batchWriteItems(writeItemList, full_tableName, dynamodbClient);
          break;
        default:
          throw new TypeError(
            `unknown query mode ${mode}. Please double check your input.`
          );
      }
      return result;
  
    
  }