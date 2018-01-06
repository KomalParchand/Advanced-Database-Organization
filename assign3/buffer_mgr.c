//////////////////////  TEAM 05  /////////////////////////////


#include "buffer_mgr.h"
#include <stdio.h>
#include <stdlib.h>
#include "dberror.h"
#include "storage_mgr.h"
#include <string.h>
#include "frame.h"





// The function initBufferPool is used to create a buffer pool for an existing page file 
// bm = management data for buffer pool is stored
// pageFileName = contains the name of the file whose data is to be cached from memory
// numPages = indicates total number of frames in the given buffer pool
// strategy = indicates the page replacement stratergy to be used (FIFO, LRU)
// stratData = indicates parameter needed for page replacement intially value is NULL

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy,void *stratData)
{
	int i=0;
	countWrite=0;
    countRead=0;
	//SM_FileHandle *fhandle;								  //Storage manager file handle
	fhandle = (SM_FileHandle *)malloc(sizeof(SM_FileHandle));
	char* bpm = (char *)calloc(PAGE_SIZE*numPages,sizeof(char));
	bm->pageFile = (char*)pageFileName;

	/*BPool_Mgmt *bpm;
	bpm = (BPool_Mgmt*)malloc(sizeof(BPool_Mgmt));  //allocate memory for buffer bpool management
	do
	{
	createframe(bpm);								//creating  the  numPages number of frames for buffer pool to be managed
	i++;
	}while(i<numPages);								
	*/
	bm->numPages = numPages;					//initialising the values and store it in management data
	bm->strategy = strategy;
	//bpm->end = bpm->header;	
	//bpm->nonempty = 0;
	//bpm->totalRead = 0;
	//bpm->totalWrite = 0;
	//bpm->stratData = stratData;
	bm->mgmtData = bpm;
	fq= (framequeue *)malloc(sizeof(framequeue));
	openPageFile(bm->pageFile,fhandle);			//open the page file, whose pages are to be cached
	createframe(fq,bm);
	//closePageFile(&fhandle);							//close the page file
	return RC_OK;
}


// The shutdownBufferPool function is used to hutdown a buffer pool and free up all associated resources to avoid memory leaks
// Apart from this the dirty pages are written pushed to be written on disk before deleting them
 
RC shutdownBufferPool(BM_BufferPool *const bm)
{
	
/*	BPool_Mgmt *bpm ;
	bpm = bm->mgmtData;			//load the management data of the buffer pool

	Frame *frame;
	frame = bpm->header;		//pointing to the head (starting) node of frames linked list
  
*/
	forceFlushPool(bm);			// forceFlush is called to write all the dirty Pages back again, before destroying the file

 /*  	for(;;)
	{
		free(frame->data);
		frame= frame->next;
		if(frame==bpm->header) break;
	}
	
	bpm->start = NULL;									//re-intialise the values to NULL or 0
	bpm->header = NULL;
	bpm->end = NULL;

	free(frame);									  		//free the entire frame
	free(bpm);								   	   			//free the bufferPool
*/
	//bm->mgmtData = NULL;
	//bm->numPages = 0;
	//bm->pageFile = NULL;								//setting all the values to 0 or NULL
	free(bm->mgmtData);
	closePageFile(fhandle);
	return RC_OK;
}

//The forceFlushPool function  force the buffer manager to write all dirty pages to disk 

RC forceFlushPool(BM_BufferPool *const bm)
{
	//SM_FileHandle filehandle;
	//BPool_Mgmt *bpm;
	//bpm= bm->mgmtData;								//load the management data of the buffer pool
	int res;
	Frame *frame;
	//frame = bpm->header;							//pointing to the head (starting) node of frames linked list
	frame = fq->front;
//	char openfile = openPageFile ((char *)(bm->pageFile), &filehandle);
	
//	if ( openfile == RC_OK)			// if the file exsits then it opens the page file
//	{
			while(frame!=NULL)												// until frame is not eual to buffer pool header keep writing the dirty pages
		{
				res =-1;
				if(frame->dirty == 1)		//if dirty =1 and fix count =0 , page can be written to disk
					{
			    		res=writeBlock(frame->pageNum, fhandle, frame->data) ;
							countWrite++;	
							frame->dirty = 0;							//mark the dirty 0
								//bpm->totalWrite++;							//increement the count of total number of pages written in buffer pool
							if (res!=0)
            				{
                				closePageFile(fhandle);						//close the pageFile
								return RC_WRITE_FAILED;
            				}	
									
					}
				frame = frame->next;										//moves to next frame in the list of frames
	//	if(frame == bpm->header) break;
		
		}

//	closePageFile(&filehandle);														//closes the page file by calling function of storage manager
	return RC_OK;
		
	}

/*else
{
	return RC_FILE_NOT_FOUND;						// if the file does not exsit then it return the error message " File not found"
}
  */


/* The markDirty function is used to mark the page as dirty
 the dirty bit is initialised to 1 if it is dirty
*/
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	//BPool_Mgmt *bpm;
	//bpm=bm->mgmtData;								//load the management data of the buffer pool
	Frame *frame;
	frame=searchFrame(bm,page);								//pointing to the head (starting) node of frames linked list

	//for(;;)
	//{
		if(frame != NULL)
		{
			frame->dirty = 1;								//dirty bit is marked to the page in frame
			return RC_OK;
		}
		//frame=frame->header;
		
	  //if(frame==bpm->header) break;	
//	}
}

 // The unpinpage function is used to unpin page.
// The pageNum field of page should be used to figure out which page to unpin.


RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	//BPool_Mgmt *bpm=bm->mgmtData;				//load the management data of the buffer pool
	Frame *frame;
	frame=searchFrame(bm,page);;				//pointing to the head (starting) node of frames linked list
	
//	for(;;)									// the loop is used to unpin the pages from the buffer pool once they are wrtten back on disk.
//	{
		frame->fixCount--;			//fixCount is decremented
		if(*(frame->data)==*(page->data))		
		{
			return RC_OK;
		}
	//	if(frame==bpm->header) break;
//	}
//	return RC_OK;
}

// The forcePage function is used to write the current content of the page back to the page file on disk

RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	//BPool_Mgmt *bpm=bm->mgmtData;
	Frame *frame = searchFrame(bm,page);
	int i=0;
//	char p =openPageFile ((char *)(bm->pageFile), &filehandle);
	//	if (p == RC_OK)									// PageFile  is opened in write mode if the file exsists
	//{    
	//	  frame->dirty == 1	
		//if(frame==bpm->header)
	//		{
		//	do{
		//		if (frame->pageNum == page->pageNum)					// checks if thee condtents of the page are changed by client
		//		{
			
				if(writeBlock(frame->pageNum, fhandle, frame->data) == RC_OK)		//write operation is performed for the dirty page
					{		
					countWrite++;											//	number of write performed are incremented
					//frame->dirty = 0;											//dirty bit is set back to 0 (unmarked)
					return RC_OK;
					
					}
					else
					{
						closePageFile(fhandle);										//closes the pageFile()
						return RC_WRITE_FAILED;
					}
			//	}
			//	else
			//	{
			//		frame=frame->header;
			//	}
			  // i++;
		   	//frame= frame->header;
			//}while(i<fq->framesize);
	
}
	
	//else
	//{
	//			return RC_FILE_NOT_FOUND;							//if PageFile doesnt exsists error message is written "File not found"

	//}

	//closePageFile(&filehandle);
	//return RC_OK;
//}
//}

// The pinPage  function pins the page with page number (pageNum). 
//It is the responsibilty of buffer manager to set the page number (pageNum ) of the page handle passed to the method. 
//Based on the strategy different page replacement algorithms are selected 

RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum)
{

	page->pageNum=pageNum;
    if(bm->strategy==0)
    {
    	Frame *tempframe;
    	tempframe=fq->front;
    	int found =0;    //set a flag to test if the given page is located in the buffer
    	if(!isEmpty(fq) && tempframe->pageNum==(int)pageNum)
        	found=1;
        int i=1;
        
    	do{
        	tempframe=tempframe->next;
        	if(tempframe->pageNum==pageNum)
            found=1;
            i++;
    	 }while(i<bm->numPages&&found==0);
    
     if(found==1)
   	 {
        tempframe->fixCount++;
        page->data=tempframe->data;
        return RC_OK;
    }
    Frame *newFrame=(Frame *) malloc (sizeof(Frame));
    page->pageNum=pageNum;
    int count=bm->numPages;
    Frame* tempPtr=fq->front;
    Frame* tempPtr_pre=fq->front;
    newFrame->pageNum=pageNum;
    newFrame->dirty=0;
    newFrame->fixCount=1;
    newFrame->next=NULL;
    
    // remove one page from the buffer (considering the dirty page and the page which is occupied)

    while(tempPtr->fixCount>=1)                          //if some one is occupying the page, move to the next
        {                                                   //until find the free page.
            if(count <=0)
                return RC_FILE_NOT_FOUND;             //if all the buff is occupied, return the error
            tempPtr_pre=tempPtr;
            tempPtr=tempPtr->next;
            count--;
        }
    if(tempPtr==fq->front)                         //if the first page in the buff is free(fixCount==0)
            fq->front=tempPtr->next;                   // set the front pointer of queue to the second page
    else
           tempPtr_pre->next=tempPtr->next;                 //else let the previous page's next pointer points to
    
	newFrame->data=tempPtr->data;
                                                            // the next one(skip the page between them)
    if(tempPtr==fq->prev)
            fq->prev=tempPtr_pre;
       
    if(tempPtr->dirty==1)                               // if the page is dirty, write it to the file
        {
            int resul = writeBlock(tempPtr->pageNum,fhandle, tempPtr->data);
            countWrite++;
            if (resul !=0)
     		return RC_FILE_NOT_FOUND;
        }
    
	newFrame->frameno=tempPtr->frameno;
    free(tempPtr);                                      // free the node.
    fq->framesize= fq->framesize-1;           //decrease the size of queue by one
 
    
    // add the new page to the buffer(the rear of the queue)
   	int resul= readBlock(pageNum, fhandle, newFrame->data);
    page->data=newFrame->data;
    countRead++;
    if (resul !=0)
    return RC_FILE_NOT_FOUND;
	fq->prev->next=newFrame;
    fq->prev=newFrame;
    Frameinfo[newFrame->frameno]=newFrame;
    fq->framesize= fq->framesize+1;           //increase the size of queue by one


    return RC_OK;
	}
			

    else //if(bm->strategy==1)

	{
		
					
					/*openPageFile((char*)bm->pageFile,&filehandle);			// opens the PageFile if it exsits
					for(;;)
					{	int p=frame->pageNum;
						if(p == pageNum)										//checks if the pageNum equals the Page present in the Page frame
						{
							page->pageNum = pageNum;
							page->data = frame->data;
				
							frame->pageNum = pageNum;
							frame->fixCount++;
				
							bpm->end = bpm->header->next;					//pointer pointing to the position for page replacement
							bpm->header = frame;
							return RC_OK;
						}
				
						frame = frame->next;
					if(frame== bpm->header)
					break;
					}
				
					if(bpm->nonempty < bm->numPages)			//checks the condition to see if buffer pool has empty spaces for the pages to be pinned in
					{
				
						frame = bpm->header;
						frame->pageNum = pageNum;
				
						if(frame->next != bpm->header)			//points  the header to next availble empty space in buffer pool
						{
							bpm->header = frame->next;
						}
						frame->fixCount++;		//increment the fix count
						bpm->nonempty++;	//increment the non empty Count
					}
					else									
					{
						frame = bpm->end;
						for(;;)
						{
							if(frame->fixCount == 0)			//check if the page is in use 
							{	
								if(frame->dirty == 1)			// if page is not in use check for dirty page condition if dirty=1 write page back to disk before deleting
								{
									ensureCapacity(frame->pageNum, &filehandle);			//capacity increased if required pages are less
									char writeBlk = writeBlock(frame->pageNum,&filehandle, frame->data);  //page written on the disk
									if(writeBlk==RC_WRITE_FAILED)
									{
										closePageFile(&filehandle);
										return RC_OK;
									}
									bpm->totalWrite++;	//increase the total number of writes to disk
								}
								if(bpm->end == bpm->header)				//implement the  page replacement strategy --LRU
								{		frame = frame->next;
									frame->pageNum = pageNum;
									frame->fixCount++;
									bpm->end = frame;
									bpm->header = frame;
									bpm->end = frame->prev;
									break;
									
								}
								else
								{
									frame->pageNum = pageNum;
									frame->fixCount++;
									bpm->end = frame;
									bpm->end = frame->next;
									break;
								}
							
							}
							else								//if the page is in use then goto next frame whose fix count = 0, and replace the page
							{
								frame = frame->next;
							}
						if(frame== bpm->end)
						break;	
						}
					}
				
					ensureCapacity((pageNum+1),&filehandle);						//increase the total number of reads to disk
					char readBlk =readBlock(pageNum, &filehandle,frame->data) ;			//read bock into the frame
					if(readBlk==RC_READ_NON_EXISTING_PAGE)
					{
						return RC_READ_NON_EXISTING_PAGE;
					}
					bpm->totalRead++;												//increase the total number of reads to disk
					page->pageNum = pageNum;
					page->data = frame->data;
					closePageFile(&filehandle);										//close the PageFile
				
					return RC_OK;
						
		
	}
*/	
	Frame *tempframe;
    Frame *temp_pre;
    Frame *temp1;
    Frame *temp_pre1;
    temp_pre=fq->front;
    tempframe=fq->front;
    temp_pre1=fq->front;
    temp1=fq->front;
    int found =0;
    
	if(!isEmpty(fq) && tempframe->pageNum==(int)pageNum) 
        found=1;
        int i =1;
   
    do {
        temp_pre=tempframe;
        tempframe=tempframe->next;
        if(tempframe->pageNum==pageNum)
            found=1;
            i++;
    }while(i<fq->framesize&&found==0);
    
    if(found!=1)
    {
    	bm->strategy=0;
	    return pinPage(bm,page,pageNum);
    }
    else
    {
    	tempframe->fixCount++;
        page->data=tempframe->data;
        if(tempframe!=fq->front)
        {
            temp_pre->next=tempframe->next;
            if(tempframe==fq->prev)
        	fq->prev=temp_pre;
            fq->prev->next=tempframe;
            fq->prev=tempframe;
            
        }
        else
        {
                        
            fq->front=tempframe->next;
            fq->prev->next=tempframe;
            fq->prev=tempframe;
            
        }
        tempframe->next=NULL;
        Frameinfo[tempframe->frameno]=tempframe;
        return RC_OK;
	}
            
}
}

// The getFrameContents function  returns  array of PageNumbers (of size numPages)
PageNumber *getFrameContents (BM_BufferPool *const bm)
{
	//Frame *frame;
	PageNumber (*memoryallocate)[bm->numPages];
	int countofpages = bm->numPages,i=fq->framesize-1;
	//BPool_Mgmt *bpm = bm->mgmtData;							//load the management data of the buffer pool
	memoryallocate = calloc(bm->numPages,sizeof(PageNumber));				//allocate memory for numPages
//	bpm->frameContent = memoryallocate;
//	frame = bpm->start;									//pointing to the head (starting) node of frames linked list
//	fContent = bpm->frameContent;
								
//	if(fContent == NULL)
//	{
	//	return RC_FRAME_NOT_FOUND;
//	}
//	else
//	{
	
		while(i>=0)
		{
			(*memoryallocate)[i] = Frameinfo[i]->pageNum;					
			//frame = frame->next;
			i--;
		}
	
//	}
	
	return *memoryallocate;
}


 // The getDirtyFlags function returns an array of bools (of size numPages)
 
bool *getDirtyFlags (BM_BufferPool *const bm)
{
	Frame *frame;
	bool (*memoryallocate1)[bm->numPages];
	int countofpages=bm->numPages,i=fq->framesize-1;
	//BPool_Mgmt *bpm=bm->mgmtData;							//load the management data of the buffer pool
	memoryallocate1= calloc(bm->numPages,sizeof(PageNumber));			//allocate memory for numPages
	//bpm->dirtypage=dirtyp;
	//frame = bpm->start;											//pointing to the head (starting) node of frames linked list
	//dirtyp= bpm->dirtypage;


//	if(dirtyp == NULL)
//	{
//		return RC_FRAME_NOT_FOUND;
//	}
//	else
//	{
	
		while(i>=0)
		{
			if(Frameinfo[i]->dirty==1)
            (*memoryallocate1)[i]=1;
        else
            (*memoryallocate1)[i]=0;
            
         i--;   
		}
//	}

	return *memoryallocate1;
}

//  The getFixCounts function returns an array of ints (of size numPages)
int *getFixCounts (BM_BufferPool *const bm)
{
	Frame *frame;
	int (*memoryallocate2)[bm->numPages];
	int countofpages = bm->numPages,i=fq->framesize-1;
	//BPool_Mgmt *bpm= bm->mgmtData;								//load the management data of the buffer pool
	memoryallocate2 = calloc(bm->numPages,sizeof(PageNumber));		//allocate memory for numPages
	//bpm->fixCount=fix;
	//frame = bpm->start;							//pointing to the head (starting) node of frames linked list
	//fix = bpm->fixCount;


//	if(fix== NULL)
//	{
//		return RC_FRAME_NOT_FOUND;
//	}
//	else
//	{
	
		while(i>=0)
		{
			(*memoryallocate2)[i] = Frameinfo[i]->fixCount;
		//	frame = frame->next;
			i--;
		}
//	}
	return  *memoryallocate2;
}

// This function read from disk and returns number of pages
int getNumReadIO (BM_BufferPool *const bm)
{
		//BPool_Mgmt *bpm;
		//bpm= bm->mgmtData;					//load the management data of the buffer pool
		return(countRead);				// returns number of read pages
		
}

// This function writes the page file and returns the number of pages

int getNumWriteIO (BM_BufferPool *const bm)

{
	 //BPool_Mgmt *bpm;
	 //bpm = bm->mgmtData;				//load the management data of the buffer pool
	 return(countWrite);			// returns  number of write pages
	 
}