//lang::CwC

#pragma once

class Object {
	public:
		Object() {}
		virtual ~Object() {}
	    virtual size_t hash() { return reinterpret_cast<size_t>(this); }
	    virtual bool equals(Object* object) { return object == this; }
	    virtual Object* clone() { return new Object(); }
};
