
////////////////// TEAM 05 //////////////////////

#include "storage_mgr.h" 
#include <stdio.h> 
#include <stdlib.h> 
#include <string.h> 
#include "dberror.h"
void initStorageManager (void) 
{ 
 
/* manipulation of page files */ 
 
} 
 
/* 
	The page called filename is created by this function The initial size of the file is set to 1 page
    Every single page consist of "\0" bytes  
 */ 
RC createPageFile (char *fileName) 
{ 
    FILE *ptr;        //pointer pointing to file
 
    char *start, *header; 
 
    ptr = fopen(fileName, "wb");    //file is opened in write mode 
 
    if(ptr==NULL)    // if condtion satisfies
    { 
        return RC_FILE_NOT_FOUND;     // throws error file not found  
    } 
    else 
    { 
        start = (char*)calloc(PAGE_SIZE,sizeof(char));  // allocating memory of information at start of the page 
 
        header = (char*)calloc(PAGE_SIZE,sizeof(char)); //allocating memory for storing  file information 

        fwrite("1",sizeof(int),1,ptr); // page number is initialise to 1 for file pointed by ptr
 
        fwrite(header,sizeof(char), PAGE_SIZE, ptr); //  total no. of Pages is written with header 
        
        fwrite(start,sizeof(char), PAGE_SIZE, ptr); // firstpage written with "\0" bytes 
        
        free(header);         // deallocates the memory

        free(start);    // deallocates the memory
 
        fclose(ptr);    //Close the file 
 
        return RC_OK; 
    } 
} 
 
/* this method opens an existing page file. It return RC_FILE_NOT_FOUND if the file does not exist. 
	If file exsist then it assigns the file handle with information like number of pages, curPagePos etc.  
 */
RC openPageFile (char *fileName, SM_FileHandle *fHandle) 
{ 
    FILE *ptr;          //pointer pointing to file
 
    ptr = fopen(fileName, "r+");    // opening the page in read plus mode where the file can be read as well as written
 
 	long lengt;
    
	long tailPtr = -1;
    
    if(ptr!=NULL)   
    { 
 
 		fseek(ptr,0L,SEEK_END);  
 		 
 		tailPtr = ftell(ptr);
		 
		 if(tailPtr !=1)
		 
		 {	
		 
			lengt=tailPtr+1; 	  
        
			fHandle->fileName = fileName;    //storing file name 
 
        	fHandle->totalNumPages = 1; // assigning total number of pages as 1 
        
        	fHandle->curPagePos = 0;    // storing curPagePos  
 
        	fHandle->mgmtInfo = ptr;   //store the information about the file pointer 
 
        	return RC_OK;
		}
    } 
    else    
    { 
        return RC_FILE_NOT_FOUND; 
    } 
} 
 
/* 
   This method is used to close the pageFile 
 */ 
RC closePageFile (SM_FileHandle *fHandle) 
{ 
   // FILE *mfile = fopen(fHandle->fileName,"r");  // opens the file in read mode
    
   // printf("\n File is closing....!"); 
     							
	//fclose(fHandle->mgmtInfo);		// function fclose() is used to close the file.
	//return RC_OK; 

if(fclose(fHandle->mgmtInfo)==EOF)          //if could not find the file ,return NOT FOUND.
        return RC_FILE_NOT_FOUND;
    else
        return RC_OK;
	    
} 
 
/* 
  This method is used to delete the pageFile or destory the pageFile 
 */ 
RC destroyPageFile (char *fileName) 
{ 
    int value; 
     
    value = remove(fileName); 	//remove() in-built functin is used to destroy the file
      
    if(value != -1) 
    { 
        return RC_OK;				// returns RC_OK if successfully destroyed 
    } 
    else 
    { 
      perror("\n Error in deleting a file");  	 // prints the error message if unsucessful
    } 
} 
 
/* 
   This method  is used to read the pageNum(th) block from a file and 
   stores its content in the memory pointed to by the memPage page handle.
   It return RC_OK if the block is read successfully. If the file has less than pageNum pages, it return RC_READ_NON_EXISTING_PAGE. 
 */ 
RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) 
{ 
 
 
  if (pageNum < fHandle->totalNumPages || pageNum > 0)  		//checks the condition if the pageNum is valid page number
    { 
          fseek(fHandle->mgmtInfo,PAGE_SIZE*pageNum*sizeof(char), SEEK_SET); 	//poiints to file pointed by mgmtInfo at Pagenum from begining of the file
          
          fread(memPage,sizeof(char),PAGE_SIZE,fHandle->mgmtInfo); 		// reads from the memory from the position pointed by fseek() function
          
          printf("\n Current Page Position: %d",pageNum);
          
          fHandle->curPagePos=pageNum; // get the current page position and point the cursor to start reading 
          
        	return RC_OK;      
    } 
  	else 
    { 
      return RC_READ_NON_EXISTING_PAGE;    
    }    
} 
/* 
  This method is used to return the current page position 
 */ 
int getBlockPos (SM_FileHandle *fHandle) 
{ 
    printf ("\n Current Page Position : %d", fHandle->curPagePos);    // display the current page position 
    
    printf ("\n Total no of pages : %d", fHandle->totalNumPages);		// display the total number of pages 
    
    return fHandle->curPagePos; 										// returns the current page position in the file
} 
 
/* 
   This method is used to read the First Block from the pageFile into memPage. 
   It returns an error if there are no pages in the pageFile 
 */ 
RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) 
{ 
    char readblk= readBlock(0,fHandle,memPage);			// readBlock() method is called with pageNum set to ) indicating the first page
    
    if(readblk==RC_OK) 
    
        return RC_OK;
        
    else
    
        return RC_READ_NON_EXISTING_PAGE; 
     
} 
 
/* 
   This method is used to read the previous Block from the pageFile into memPage.
   It returns an error RC_READ_NON_EXISTING_PAGE if no previous block is found in the pageFile 
 */ 
RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) 
{
    int pageNum;
    
    pageNum = fHandle->curPagePos;    		// pageNum is used to store the current page position value
    
    if (pageNum < fHandle->totalNumPages || pageNum > 0) 		//checks the condition if the pageNum is valid page number
     { 
        fseek(fHandle->mgmtInfo,(pageNum-1)*PAGE_SIZE*sizeof(char), SEEK_SET);   //fseek() function points to previous page relative to value in pageNum (pageNum-1)
        
        fread(memPage,sizeof(char),PAGE_SIZE,fHandle->mgmtInfo); 	// reads from the memory from the position pointed by fseek() function
        
        printf("\n Current Page Position: %d",pageNum-1);				// displays the page num of the page read from the file.
        
        return RC_OK; 
     } 
  else 
    { 
      return RC_READ_NON_EXISTING_PAGE;    
    } 
 }  
 
/* 
  This method is used to read the Current Block from the pageFile into memPage.
  It returns an error RC_READ_NON_EXISTING_PAGE if there are no blocks in the pageFile 
 */ 
RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) 
{   
     char readblk =readBlock(fHandle->curPagePos,fHandle,memPage); // to retrive the current block we make use of getBlockPos() by passing thenfHandle
     
        if(readblk == RC_OK)
        
            return RC_OK;
            
        else
        
            return RC_READ_NON_EXISTING_PAGE;
 
} 
 
 
/* 
  This method is used to read the First Block from the pageFile into memPage. 
  It returns an error RC_READ_NON_EXISTING_PAGE if there are no pages in the pageFile 
 */ 
RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) 
{ 
    int pageNum;
    
    pageNum = fHandle->curPagePos; 				// pageNum is used to store the current page position value
    
   if (pageNum < fHandle->totalNumPages || pageNum > 0) 		//checks the condition if the pageNum is valid page number
    { 
        fseek(fHandle->mgmtInfo,(pageNum+1)*PAGE_SIZE*sizeof(char), SEEK_SET); //fseek() function points to next page relative to value in pageNum (Pagenum+1)
        
        fread(memPage,sizeof(char),PAGE_SIZE,fHandle->mgmtInfo); 	// reads from the memory from the position pointed by fseek() function
        
        printf("\n Current Page Position: %d",pageNum+1);				// displays the page num of the page read from the file
        
        return RC_OK; 
    } 
  else 
    { 
      return RC_READ_NON_EXISTING_PAGE;    
    } 
 } 
 
/* 
  This method is used to read the Last Block from the pageFile into memPage.
  It returns an error RC_READ_NON_EXISTING_PAGE if there are no pages in the pageFile 
 */ 
RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) 
{ 
  char readblk =readBlock(fHandle->totalNumPages,fHandle,memPage);  // to read the last block we pass the value of pageNum as totalNumPages-1 to readBlock() function 
    
	if(readblk == RC_OK)
    
	    return RC_OK;
    
	else
    
	    return RC_READ_NON_EXISTING_PAGE; 
} 
 
/*
 This method consist of block in which we can write based on the pageNum field. 
 Error RC_WRITE_FAILED will be displayed if block is not found
*/ 

RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) 
{ 
	int value;
	     
//	FILE *mfile; 
	
//	mfile = fopen(fHandle->fileName, "w");  // opens the file in write mode

		while((fHandle->totalNumPages)<=pageNum)
        
		appendEmptyBlock (fHandle);
	
//	if(pageNum < fHandle->totalNumPages  || pageNum > 0) 
  //  {      
		fseek (fHandle->mgmtInfo,(PAGE_SIZE) * (pageNum)*sizeof(char),SEEK_SET);  //fseek() function points to next page relative to value in pageNum (Pagenum+1)
		
		value =fwrite ( memPage,sizeof(char), PAGE_SIZE , fHandle->mgmtInfo);   // writes the value into the PafeFile 
		
				
		fHandle->curPagePos=pageNum; 
		
		return RC_OK ;
		
	//}
	
} 
/* 
The method writes the current pointed block.
Error RC_WRITE_FAILED will be displayed if block is not found 
*/ 
RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) 
{ 
    int newposition; 
    
	newposition = getBlockPos(fHandle); 
    
	char writeblk= writeBlock (newposition, fHandle, memPage);       //writing the current block 
    
	if(RC_OK == writeblk) 
    
		return RC_OK; 
    
	else 
    
	    return RC_WRITE_FAILED; 
} 
 
/* 	
 This method is used to append a new Empty block into the existing pageFile containing '\0' bytes 
*/ 
RC appendEmptyBlock (SM_FileHandle *fHandle) 
{ 
    char * emptyPage; 
   
    emptyPage = (char*)calloc(PAGE_SIZE, sizeof(char)); // allocating memory of information for new empty page 
   
    int nextPage= fHandle->totalNumPages+1; 
 
    //fseek(fHandle->mgmtInfo,nextPage*PAGE_SIZE,SEEK_SET);    //points to file pointed by mgmtInfo at totalNumPage from begining of the file

    if(!fwrite(emptyPage, sizeof(char),PAGE_SIZE,fHandle->mgmtInfo))  //if write operation is not possible,it displays error otherwise append an empty page 
    { 
		free(emptyPage); 
      
	    return RC_WRITE_FAILED;  
    } 
    else 
    { 
		fHandle->totalNumPages = nextPage;        //update the total number of pages 
		
       // fHandle->curPagePos = fHandle->totalNumPages - 1;    //update the current page position 
        
      //  fseek(fHandle->mgmtInfo,0,SEEK_SET); //points to the position pointed by the header
        
	  //	fwrite(emptyPage, PAGE_SIZE, 1, fHandle->mgmtInfo);	// writes in empty page	
		
      //  fseek(fHandle->mgmtInfo,nextPage*PAGE_SIZE,SEEK_SET);   //points to the position where the pointer was last pointing 

        free(emptyPage); // to avoid memory leaks the memory is made free 
        
        return RC_OK; 
		 
    } 
} 
 
/*  Method cosnist of ensuring capacity of pagefile in this method the numberofpages is given and
    if the capacity is not achieve then the difference of pages is added to the capcacity
 */ 
RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle) 
{ 
    if(fHandle->totalNumPages <= numberOfPages) // capacity is maintained or not is checked in this if condition 
    { 
            int i=0;
        
		    int additionalpages; 
        
		additionalpages = numberOfPages - fHandle->totalNumPages; //the number of pages  to appended is calculated 
        
        while(i<additionalpages) // in this loop the block is append to ahcieve the reqiured capacity  
        { 
            appendEmptyBlock(fHandle); // appendEmptyBlock function is called 
            i++; 
        } 
        return RC_OK;
    } 
    
}