#include "Vertica.h"
#include "StringParsers.h"
#include "csv.h"
#include <string>

using namespace Vertica;
using namespace std;

template <class StringParsersImpl>
class ReproducerParser : public UDParser {
public:
    SizedColumnTypes colInfo;
    StringParsersImpl sp;
    struct csv_parser parser;
    std::vector<std::string> formatStrings;

    virtual StreamState process(ServerInterface &srvInterface, DataBuffer &input, InputState input_state) {
 
	srvInterface.log("Reproduce-Log: Receive stream input.size=%zu, input.offset=%zu, input_state=%d\n",
				 input.size, input.offset, input_state);
	while( input.offset < input.size ) {
		size_t lineLen = 0;
		size_t maxLen = input.size - input.offset;
		const char* start = input.buf + input.offset;
		bool meetNewline = false;

		while (!meetNewline && lineLen < maxLen) {
			char current = start[lineLen];
			if (current == '\n') {
				meetNewline = true;
			}
			++lineLen;
		}

		// If input buffer does not containt the whole row, return INPUT_NEEDED
		if (!meetNewline && lineLen == maxLen) {
			return input_state == END_OF_FILE ? DONE : INPUT_NEEDED;
		}
		// Else: code to parse row will goes here
		

		// Update input.offset
		input.offset = min(input.offset + lineLen, input.size);
		srvInterface.log("Reproduce-Log: Set input.offset=%zu\n", input.offset);
	}

	return  input_state == END_OF_FILE ? DONE : INPUT_NEEDED;
    }

    virtual void setup(ServerInterface &srvInterface, SizedColumnTypes &returnType);
    virtual void destroy(ServerInterface &srvInterface, SizedColumnTypes &returnType) {
        csv_free(&parser);
    }
};

template <class StringParsersImpl>
void ReproducerParser<StringParsersImpl>::setup(ServerInterface &srvInterface, SizedColumnTypes &returnType) {
    csv_init(&parser, CSV_APPEND_NULL);
    colInfo = returnType;
}

template <>
void ReproducerParser<FormattedStringParsers>::setup(ServerInterface &srvInterface, SizedColumnTypes &returnType) {
    csv_init(&parser, CSV_APPEND_NULL);
    colInfo = returnType;
    if (formatStrings.size() != returnType.getColumnCount()) {
        formatStrings.resize(returnType.getColumnCount(), "");
    }
    sp.setFormats(formatStrings);
}

template <class StringParsersImpl>
class ReproducerParserFactoryTmpl : public ParserFactory {
public:
    virtual void plan(ServerInterface &srvInterface,
            PerColumnParamReader &perColumnParamReader,
            PlanContext &planCtxt) {}

    virtual UDParser* prepare(ServerInterface &srvInterface,
            PerColumnParamReader &perColumnParamReader,
            PlanContext &planCtxt,
            const SizedColumnTypes &returnType)
    {
        return vt_createFuncObject<ReproducerParser<StringParsersImpl> >(srvInterface.allocator);
    }
};

typedef ReproducerParserFactoryTmpl<StringParsers> ReproducerParserFactory;
RegisterFactory(ReproducerParserFactory);

typedef ReproducerParserFactoryTmpl<FormattedStringParsers> FormattedReproducerParserFactory;
RegisterFactory(FormattedReproducerParserFactory);
