
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include "record_mgr.h"
#include "tables.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "expr.h"
int rows =0;
int countofSlot=0;
int currentPage=0;
int numPageHeader=0;
int size=0;
int count=0;
int pgsize=PAGE_SIZE-2;

Expr * condition;
BM_PageHandle *page;
SM_FileHandle fhandle;
BM_BufferPool *bm;

typedef struct RM_INFO                      				 // here I define a struct to encapsulte the record address and counter number
{
    Record *record;
    int count;
}RM_INFO;


// this function is to transfer the schema struct to string space
SM_PageHandle ConvertToString(Schema *schema)
{
    char * page=NULL;
    int strLen=0,i=0,j=0,k=0,l=0;                            //the length of all lines of strings (end by '\0')
    int offset=0;
    page=(char *)malloc(PAGE_SIZE*sizeof(char)); 			//allocate the memory to store the information of the table
    memset(page, 0, PAGE_SIZE);
    *((int *)(page+offset))=schema->numAttr;
    offset= offset + sizeof(int);
    while(i<schema->numAttr)               					//loop through the numAttr to append the attributes names with the page.
    {
        strLen=strlen(schema->attrNames[i])+1;
        memcpy(page+offset,schema->attrNames[i],strLen);
        offset=offset + strLen;
        i++;
    }
    while(j<schema->numAttr) {             					//loop through the numAttr to append the data type with the page
        *((int *)(page+offset)) =schema->dataTypes[j];
        offset=offset + sizeof(int);
        j++;
    }
    while(k<schema->numAttr)
    {
    *((int *)(page+offset)) =schema->typeLength[k];       //loop through the numAttr to append the type length with the page.
    offset= offset + sizeof(int);
    k++;
    }
    while(l<schema->numAttr)
    {
    *((int *)(page+offset)) =schema->keyAttrs[l];         //loop through the numAttr to append the key attribute with the page.
    offset= offset + sizeof(int);
    l++;
    }
    *((int *)(page+offset))=schema->keySize;            //append the key size with the page.
    offset= offset + sizeof(schema->keySize);
    return page;
}


//this function is used to transfer the string into struct
Schema *ConvertToSchema(SM_PageHandle ph)
{
    int offset=0,j=0,k=0,l=0;
    int count=0;
    char buff[255];
    Schema *schema = malloc(sizeof(Schema));
    schema->numAttr= *((int *)(ph+offset));
    offset=offset + sizeof(int);
    schema->attrNames=(char **)malloc(sizeof(char*)*schema->numAttr);
    for(int i =0;i<schema->numAttr;i++)                                     //loop through the numAttr to assign the attributes name to the **attrName
    {
        buff[255]="";
        count=0;
        while(*(ph+offset++)!='\0')
        {
            buff[count++]=*(ph+offset-1);

        }
        schema->attrNames[i]=(char *)malloc(count*sizeof(char)+1);
        strcpy(schema->attrNames[i],buff);
    }
    schema->dataTypes=(DataType *)malloc(sizeof(DataType)*schema->numAttr); //loop through the numAttr to allocate the memory to store the dataTypes
    while(j<schema->numAttr)
    {
        schema->dataTypes[j]=(int)malloc(sizeof(int));
        schema->dataTypes[j]=*((int *)(ph+offset));
        offset= offset +sizeof(int);
        j++;
    }
    schema->typeLength=(int *)malloc(sizeof(int *)*schema->numAttr);        //loop through the numAttr to allocate the memory to store the type length
    while(k<schema->numAttr)
    {
        schema->typeLength[k]=*((int *)(ph+offset));
                offset= offset + sizeof(int);
                k++;
    }
    schema->keyAttrs=(int *)malloc(sizeof(int *)*schema->numAttr);
    while(l<schema->numAttr)                                       //loop through the numAttr to allocate the memory to store the number attributes
    {
        schema->keyAttrs[l]=*((int *)(ph+offset));
        offset= offset + sizeof(int);
        l++;
    }
    schema->keySize= *((int *)(ph+offset));
    offset=offset + sizeof(int);
    return schema;
    
}
// table and manager
RC initRecordManager (void *mgmtData){
    return RC_OK;
}
RC shutdownRecordManager (){
    return RC_OK;
}

//Creating a table should create the underlying page file and store information about the schema, free-space, ... and so on in the Table Information pages
RC createTable (char *name, Schema *schema){
	
	//Page handle from Storage Manager
    SM_PageHandle ph;
    
	//We are using the name coming from the createTable and calling the function createPageFile for creating a page file.
    (createPageFile(name));

    //Opening the page file with the given name.
    (openPageFile(name, &fhandle));
    //To check if the capacity is maintained or not
    ensureCapacity(1, &fhandle);
    
    //storing the struct schema to string space into page handle. 
    ph=ConvertToString(schema);
    
    //The method writes the current pointed block.
    if(writeCurrentBlock(&fhandle,ph)!=RC_OK){
    	
    	return RC_WRITE_FAILED;
	};
    
    //It deals with records and attribute values
     int length=0;
    attrOffset(schema,schema->numAttr, &length);
    length= length+sizeof(bool)+sizeof(bool)*schema->numAttr;      
    size=length+sizeof(RID);
    
    //It calculates the number of slots in the page
     countofSlot=(pgsize)/(2+size);            //Calculate the number of slots in the page.
    numPageHeader = sizeof(bool)+sizeof(bool)*countofSlot;
    
    //Closing the page file
    closePageFile(&fhandle);
    
    return RC_OK;
}


//We are using this function to open a created table.
RC openTable (RM_TableData *rel, char *name){
	//Allocating memory to the page handle
    page=(BM_PageHandle *)calloc(PAGE_SIZE,sizeof(BM_PageHandle));
    
    //Initializing the schema
    Schema *schema;
    
    //Initializing and making the buffer pool
    bm=MAKE_POOL();
    
    initBufferPool(bm,name, 3, RS_FIFO, NULL);
    
    //Pin the page at 0 which contains the information of necessary schema.
    pinPage(bm, page,0);
    
    //storing the string space to struct schema into schema.
    schema=ConvertToSchema(page->data);
    
    //name of the schema is stored here
    rel->name=name;
    
    rel->schema=schema;
    
    //Details of the record management is stored here.
    rel->mgmtData=bm;
    return RC_OK;
}
//In this function, we are closing the table and de-allocating all the memory allocated.
RC closeTable (RM_TableData *rel){
	
	//Free all the memory allocated related to page handle.
    free(page);
    
    //We are calling shutdownBufferPool to shutdown the buffer pool and release all the memory.
    shutdownBufferPool(bm);
    
    //Release all the memory allocations.
    free(bm);
    free(rel->schema->attrNames);
    free(rel->schema->dataTypes);
    free(rel->schema->keyAttrs);
    free(rel->schema->typeLength);
    free(rel->schema);
    
    return RC_OK;
}
//We are using this function to delete the already created table.
RC deleteTable (char *name){
	
	//we are using the in-built function remove of c to delete the table.
    if(remove(name)==0 )
        return RC_OK;
    else
        return RC_FILE_NOT_FOUND;
    return RC_OK;
}
//We are using this function to get the total number of rows in the table.
int getNumTuples (RM_TableData *rel){
    int count=0;				//initialized count to store the number of rows
    int offset=0;
    char * temp;        		    //We have initialized a temporary pointer which would point to the page content.
	int i=1;                                                
    while(i<=currentPage) {
    	//Pin the page at i which contains the information of necessary schema.
        pinPage(bm, page,i);
        
        //Value of the page data would be stored in the temporary variable.
        temp=page->data;	
        if(*(bool*)temp==true)
            count+=countofSlot;
        else
        {
            offset=sizeof(bool);      
            int j=0;
			while(j<countofSlot)                              // This condition would tell whether the page is full or not.
            {
                if(*((bool *)(temp+offset))==true)
                    count+=1;
        
			    offset=offset+sizeof(bool);
				j++;           
		    }
        }
        unpinPage(bm, page);
		i++;      //if the page is full pin next page
    }
    return count;
    
}


//HANDLING THE RECORDS IN THE TABLES

//We are using this function to insert a new record in the table.
RC insertRecord (RM_TableData *rel, Record *record){
    int offset=0;
    currentPage=page->pageNum;
    //We are checking if the pageNum is the first page or not.
    if(currentPage==0)
    {   
	
		count=0;
        currentPage++;
    }
    //Pin the page at currentPage which contains the information of necessary schema.
    pinPage(bm, page,currentPage);
    if(*(bool *)page->data==true)           //We have written the condition which would check if the page is full, open the next page
    {
	
      	//We need to unpin the page 
	    unpinPage(bm, page);
        count=0;
        currentPage++;
        //Pin the page at currentPage which contains the information of necessary schema.
        pinPage(bm, page, currentPage);
	}
    offset+=sizeof(bool)+count*sizeof(bool);                   // This step would locate the address after skipping the first boolean space
    *((bool *)(page->data+offset))=true;           // We would check the slot's representative bool and set it to be full (true)
	    if(count>=countofSlot)
        return EXIT_FAILURE;
    if(count==countofSlot-1)                           //We would check if this is the last slot in the page.If the condition is true, it will set the page to be full.
        *(bool*)page->data=true;
    offset = count*size+numPageHeader;          //We are setting the offset to a specific slot position
    //We are setting the record page as the current page
    record->id.page=currentPage;
    record->id.slot=count;
    //we are copying the page with offest data to the record id page.
    memcpy(page->data+offset,&currentPage,sizeof(int));
    offset=offset + sizeof(int);
    memcpy(page->data+offset,&count,sizeof(int));
    offset=offset + sizeof(int);
    //We are copying the content to the memory of page.
    memcpy(page->data+offset,record->data,size-sizeof(RID));     
    if((page->data+offset-2*sizeof(int)+size)!=(page->data+(count+1)*size+numPageHeader))   //Here we are testing whether th space allocated is corrent or not.
        return EXIT_FAILURE;
    count++;
    //We are marking the page as the Dirty Page, as the page contains a new record now.
    markDirty(bm, page);
    //We need to unpin the page which was marked as dirty and it has already been used.
    unpinPage(bm, page);
    return RC_OK;
}

//We are using this function to delete a new record in the table.
RC deleteRecord (RM_TableData *rel, RID id){
	
	if(id.page > 0)
	{
		BM_PageHandle *page = MAKE_PAGE_HANDLE();

		//We are pinning the page to mark that it is currently in use
		pinPage(bm, page, id.page);

		//We are setting the page number
		page->pageNum = id.page;

		//We are marking the page as the Dirty Page, as the page contains a new record now.
		markDirty(bm, page);

		//We need to unpin the page which was marked as dirty and it has already been used.
		unpinPage(bm, page);

	

		page = NULL;
		free(page);	//We are marking the page as the Dirty Page, as the page contains a new record now.	//We are freeing the page to avoid leaks
		return RC_OK;
	}
	else
	{
		return RC_RM_NO_MORE_TUPLES;
	}

	
    return RC_OK;
}
//We are using this function to update a new record in the table.
RC updateRecord (RM_TableData *rel, Record *record){
    int offset=0;
    //Pin the page at record id page which contains the information of necessary schema.
    pinPage(bm, page, record->id.page);
    offset=numPageHeader+record->id.slot*size;
    offset= offset + sizeof(RID);
    //We are copying the content to the memory of page.
    memcpy(page->data+offset,record->data, size-sizeof(RID));
    //We are marking the page as the Dirty Page, as the page contains a new record now.
    markDirty(bm, page);
    //We need to unpin the page which was marked as dirty and it has already been used.
    unpinPage(bm, page);
    return RC_OK;
}
//We are using this function to get a new record in the table.
RC getRecord (RM_TableData *rel, RID id, Record *record){
    int offset=0;
    //Pin the page at record id page which contains the information of necessary schema.
    pinPage(bm, page, id.page);
    countofSlot=(PAGE_SIZE-2)/(2+size);            //Calculate the number of slots in the page.
    numPageHeader = sizeof(bool)+sizeof(bool)*countofSlot;
    offset=numPageHeader+id.slot*size+sizeof(RID);
    record->id=id;
    record->data=page->data+offset;
    //We need to unpin the page which was marked as dirty and it has already been used.
    unpinPage(bm, page);
    return RC_OK;
}

// scans
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond){
    rows=getNumTuples(scan->rel);
    count=0;
    condition=cond;										//  sets the address of cond into condition
    scan->rel=rel;										// points to the data table
    RM_INFO *rinfo=(RM_INFO *)malloc(sizeof(RM_INFO));
    Record *record=malloc(sizeof(Record));				// intialise the memory of size record to hold the records
    record->data=calloc(size, sizeof(char));            //initialize the record to store the first tuple in the table
    record->id.page=1;									// sets the value of page =1
    record->id.slot=-1;									// sets the value of slot  = -1
    rinfo->count=0;
    rinfo->record=record;
    scan->mgmtData=rinfo;								// storing management data
    return RC_OK;
}
    
 /* 
 Once  the RM_ScanHandle data structure is intialised and passed as an argument to startScan. 
 it calls the RC next method which returns the next tuple that fulfills the scan condition.
 */   
RC next (RM_ScanHandle *scan, Record *record){

    Record *tmp_record = ((RM_INFO *)scan->mgmtData)->record;                //initialize a tmp record to store the each internal record.
    tmp_record->data=calloc(size, sizeof(char));
    Value **booleanvalue;                                              //allocate the booleanvalue space and set the bool value to be -1.
    value=malloc(sizeof(**value));
    (*booleanvalue)=malloc(sizeof(booleanvalue));
    (*booleanvalue)->dt=DT_BOOL;
    (*booleanvalue)->v.boolV=-1;
    while(true)
    {
        if(tmp_record->id.slot!=countofSlot-1)                        //if the previous tuple is not the last one in the page.
        {
            tmp_record->id.slot++;                        			  // increement the current slot number by 1.
        }
        else
        {
            
			tmp_record->id.page++;                                  //change the page and slot.
            tmp_record->id.slot=0;                                  //and set the slot value as 0
			
        }
        ((RM_INFO *)scan->mgmtData)->count++;                                            //every time move the slot will increase the counter.
        if(((RM_INFO *)scan->mgmtData)->count>rows)                                 	 // if the scan is completed for all rows then return RC_RM_NO_MORE_TUPLES
            return RC_RM_NO_MORE_TUPLES;
        getRecord(scan->rel, tmp_record->id,tmp_record);   								 //gets the record and store it in the tmp_record
        evalExpr(tmp_record, scan->rel->schema, condition, booleanvalue); 				 //tell whether record satisfies the condition and stores the bool into booleanvalue.
        if((*booleanvalue)->v.boolV)
            break;                                      								//if  found the tuple satisfying the condition then break

    }
    record->id.page=tmp_record->id.page;										
    record->id.slot=tmp_record->id.slot;
    memcpy(record->data,tmp_record->data,size);
    return RC_OK;
}
// this method closes the scan and frees the memory
RC closeScan (RM_ScanHandle *scan)
{
    free(scan->mgmtData);
    return RC_OK;
}

// dealing with schemas
int getRecordSize (Schema *schema){                                     //get the record's size
    return size-sizeof(RID);
}

//schema is created in this method
Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys){
    Schema *newSchema;
    newSchema=(Schema *)malloc(sizeof(Schema));
    newSchema->numAttr=numAttr;                                         
    newSchema->attrNames=attrNames;
    newSchema->dataTypes=dataTypes;
    newSchema->typeLength=typeLength;
    newSchema->keyAttrs=keys;
    newSchema->keySize=keySize;
    return  newSchema;
}
RC freeSchema (Schema *schema)
{
	free(schema);
    return RC_OK;
}


// record is created and memory is allocated
RC createRecord (Record **record, Schema *schema){
    Record* recordmgr= (Record *)malloc(sizeof(Record));    
    recordmgr->data=(char*)calloc(size, sizeof(char));
    *record=recordmgr;
    return RC_OK;
}
 // This method is used to calculate the offset  of the attributes
RC attrOffset (Schema *schema, int attrNum, int *result)
{	
    int offset = 0;				//store the offset calculated
    int attrPos = 0;
    
    while(attrPos < attrNum)				//based on the attributes there, find the offset, based on its DataTypes
    {
	
        switch (schema->dataTypes[attrPos])
    {
        case DT_STRING:
            offset =offset + schema->typeLength[attrPos];
            break;
        case DT_INT:
            offset = offset + sizeof(int);
            break;
        case DT_FLOAT:
            offset = offset + sizeof(float);
            break;
        case DT_BOOL:
            offset = offset + sizeof(bool);
            break;
    }
    attrPos++;
	}
    
    *result = offset;  // commit the final offset
    return RC_OK;
}

// earlier created record is made free
RC freeRecord (Record *record)
{
	
	record->data = NULL;	
	free(record->data);					
   	record = NULL;
	free(record);

    return RC_OK;
}
// This function is used to get the attributes in a record
RC getAttr (Record *record, Schema *schema, int attrNum, Value **value){
    int offset = 0;
    attrOffset(schema, attrNum, &offset);                   		//attribute offset is calculated
    offset=offset + sizeof(bool)+sizeof(bool)*schema->numAttr;      //headers are skipped
    Value* vvalue= (Value *)malloc(sizeof(Value));
    vvalue->dt=schema->dataTypes[attrNum];
    int attrsize;
    switch (vvalue->dt) {										// based on condition allocate the memory for float, boolean, string and int.
        case DT_BOOL:
            attrsize=sizeof(bool);
            memcpy(&(vvalue->v.boolV),(record->data)+offset,attrsize);
            break;
        case DT_FLOAT:
        	attrsize=sizeof(float);
            memcpy( &(vvalue->v.floatV),(record->data)+offset,attrsize);
            break;
        case DT_STRING:
        	attrsize=sizeof(char);
            vvalue->v.stringV=calloc(schema->typeLength[attrNum]+1,attrsize);
           memcpy( vvalue->v.stringV,(record->data)+offset,schema->typeLength[attrNum]);
            break;
        case DT_INT:
        	attrsize=sizeof(int);
            memcpy(&(vvalue->v.intV),(record->data)+offset,attrsize);
            break;
    }
    
    *value=vvalue;
    return RC_OK;
}


RC setAttr (Record *record, Schema *schema, int attrNum, Value *value
){
    int offset=sizeof(bool)+attrNum*sizeof(bool);
	int attrsize;
    int offattr;
                                                
    if (attrOffset(schema, attrNum,&offattr)==0)
    {   	                                          
    offattr+=sizeof(bool)+schema->numAttr*sizeof(bool);
    switch (value->dt)												//switch case is used for the datatype value of attribute
	 {                                                   
        case DT_BOOL:
        	attrsize= sizeof(bool);
        	bool valuebool=value->v.boolV;
            memcpy((record->data)+offattr, &(valuebool),attrsize);			// new attribute value are set and copied
            break;
        case DT_FLOAT:
        	attrsize=sizeof(float);
        	float valuefloat=value->v.floatV;
            memcpy((record->data)+offattr, &(valuefloat ), attrsize);					// new attribute value are set and copied	
            break;
        case DT_STRING:
            strcpy((record->data)+offattr,value->v.stringV);					// new attribute value are set and copied
            break;
        case DT_INT:
        	attrsize= sizeof(int);
        	int valueint= value->v.intV;
            memcpy((record->data)+offattr, &(valueint),attrsize );				// new attribute value are set and copied
            break;
    }
	}
    
    return RC_OK;

    
}

