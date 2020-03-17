Our implementation of the provided Array, Map, and Queue interfaces meets the current needs of the project to spec.

## ARRAY
Our *Array* class is a resizeable array with amortized appending to the end of the list and constant time retrieval. We provide wrapper classes for *Array* in order to support all required data types: int, float, String, and bool. We are able to pass all of our providers' tests with our implementation.</br>
Hash generation depends on what the type of the Array is. For example, in an array of Objects, we defer to each individual elements' hash() method. But, in array of items without hash methods (i.e. *int, float, bool*), we essentially combine the existing values in the list as one.

## QUEUE
Our *Queue* class provides the necessary push/pop functionality, and passes all of our providers' tests. The constructor initalizes the Queue with a capacity of 4, and will dynamically grow as necessary.</br>
We provide separate implementations of Queue for Objects and for Strings. The *StrQueue* class handles the necessary casting for return values throughout its functions, as to ensure ease of use.


## MAP
Our *Map* class uses a bucket strategy for storing key-value pairs. We use a helper *MapNode* class to hold individual key-values, which is chained toegether as a linked list which hold different pairs. </br>
A *Map* object is initalized to have an array of 16 separate buckets (*MapNode*s). This can be resized as necessary with the resize() function. resize() has internal logic to determine whether to grow or shrink based on the amount of currently stored elements.</br>
*MapNode* stores a key, value, and reference to the next node (*if any*).

# CRITIQUES

## ARRAY
The documentation was good, all the methods were clear, although it wasn't necessarily clear that the array had ownership of the stored objects.
We did have a few issues with the tests. The tests failed to handle primitives and had a number of unclear/redundant tests.</br>
Towards the beginning of the assignment the Spec pair was much more accomodating and responsive, but (understandably) as we got closer to the deadline we had to take matters into our own hands and fork to fix the remaining problems in the tests.</br>
In the end, once we fixed a few issues ourselves, we found the tests to be very comprehensive. Every method was tested in multiple ways with fail states included.

## QUEUE
The documentation was completely clear, we had no trouble implementing Queue at all. The tests made sense and the added tests continued to fill in any gaps in the functionality of queue.</br>
We didn't raise very many issues, but the other group using this interface did, and got comprehensive and clear reviews in time, as far as we could tell. The additional tests they added over time did not come out of nowhere and were helpful for finishing up our implementation. </br>
We did not need to write our own tests, we were impressed with the tests provided.

## MAP
We received an implementation at first instead of an interface and as such we delayed this section for later in the week. The documentation had changed and we were left in a much more free position to create our map. Methods were clear and corresponded well to tests except for key_set() which was then removed by the spec pair.</br>
They were semi-responsive. They fixed things but didn't explain their reasoning very well. We found it hard to communicate our issues to them such that they would resolve them clearly. They completely removed a test and method without alerting us. </br>
Their tests started out comprehensive, albeit with a couple flaws for compiling, but as the issues piled up from us and other groups they began removing tests as opposed to fixing them or tuning method specs. We have a standing pull request to resolve a number of warnings spawned by their test file, but it has not been merged as of 1/31 - 3:15pm, when we froze our code. 

