#pragma once

#include "../utils/object.h"

/*
 * A bit confused as to the purpose of this class? May be just for generalizing what the KV store can hold.
 * At this point, I think we should have our Dataframes implement Value for use in KV stores. But there's really no point for us 
 * to generalize other than winning some design point cookies from graders I guess.
 * 
 */
class Value : public Object {
public:
    
};