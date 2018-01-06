
#include <stdio.h>
#include <stdlib.h>
#include "dt.h"
#include "buffer_mgr.h"


/*Page Frame */
typedef struct Frame
{
	int frameno;		//number associated with each Page frame
	int pageNum;		//Page Number of the Page present in the Page frame
	int dirty;		//Dirty flag to determine whether page was modified by any client
	int fixCount;		//Fix count to indicate whether the page is in use by other clients
	//int referred;			//Reference bit used in CLOCK Algorithm to indicate the page being referred
	char *data;			// data present in the page
	struct Frame *next;
}Frame;

typedef struct framequeue
{
	struct Frame *front;
	struct Frame *prev;	// pointers of each frame pointing towards the next and previous frames for a given frame.
	int framesize;
}framequeue;

SM_FileHandle *fhandle;  //initial several variables
char *memPage;
int countRead=0;
int countWrite=0;
Frame *Frameinfo[100];
framequeue *fq ;

/* Buffer Pool for storage of Management Information
typedef struct BPool_Mgmt
{
	int nonempty;		//to keep count of number of nonempty frames inside the pool
	void *stratData;		//to indicate the page replacement strategies parameter
	Frame *header,*end,*start;	//track the nodes in linked list
	PageNumber *frameContent;	//page numbers  array to store the number of pages stored in the page frame
	int *fixCount;				//an array of integers to store the fix counts for a page
	bool *dirtypage;				//an array of bool's to store the  dirty bits for modified page
	int totalRead;				// total number of pages read from the buffer pool
	int totalWrite;				//total number of pages written into the buffer pool
}BPool_Mgmt;
*/

/*
This function is used to create a Buffer Pool with specified number of Page Frames
 i.e linked list of frames with some default values, with the first frame acting as the head node
 while the last acting as the tail node.
 This function is called by the initBufferPool() function passing mgmt info as the parameter*/
 
void createframe(framequeue *fq,BM_BufferPool *const bm)
{
	//Create a frame and assign a memory to it
	//Frame *frame = (Frame *) malloc(sizeof(Frame));
	Frame *frame[bm->numPages];
	int i;
	//intialise the page properties of the frames i.e each frame has a page within,
	//so properties (default page values) are applied to the frames itself
	for (i=bm->numPages-1;i>=0;i--)
	{
	//allocate memory for page to stored into the pageFrame
		frame[i]= (Frame*)malloc(sizeof(Frame));
		frame[i]->frameno = i;
	    frame[i]->data=bm->mgmtData+PAGE_SIZE*(i);
		frame[i]->dirty = 0;	//FALSE
		frame[i]->fixCount = 0;
		frame[i]->pageNum = -1;

	//frame[i]->referred = 0;
	 	if(i==bm->numPages-1)
            frame[i]->next=NULL;
        else frame[i]->next=frame[i+1];

        Frameinfo[i]=frame[i];
    }
        fq->front=frame[0];
        fq->prev=frame[bm->numPages-1];
        fq->framesize=bm->numPages;
	//initialise the pointers
//	mgmt->header = mgmt->start;

	//if it is the 1st frame make it the HEAD node of Linked List
/*	if(mgmt->header == NULL)
	{
		mgmt->header = frame;
		mgmt->end = frame;
		mgmt->start = frame;
	}
	else		//if other than 1st node, appened the nodes to the HEAD node, and make the link between these nodes
	{
		mgmt->end->next = frame;
		frame->prev = mgmt->end;
		mgmt->end = mgmt->end->next;
	}
*/
	//initialise the other pointers of the linked list
//	mgmt->end->next = mgmt->header;
//	mgmt->header->prev = mgmt->end;
}


int isEmpty(struct framequeue *fq)    //checks if the page is empty
{
    return fq->framesize ==0 ;
}


Frame *searchFrame(BM_BufferPool *const bm, BM_PageHandle *const page)
{
    Frame *temp;
    temp=fq->front;
    for(int i = 0 ; i<fq->framesize;i++)
    {
        if(temp->pageNum==page->pageNum)
            return temp;
        temp=temp->next;
           }
    return NULL;
}


