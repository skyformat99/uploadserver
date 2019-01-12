// Package logger ...
package logger

type TAIHETag string

const (
	//TAIHETagUndefined ...
	TAIHETagUndefined  TAIHETag			= " _undef"
	//TAIHETagPanic ...
	TAIHETagPanic TAIHETag				= " _panic"
	//TAIHETagHttpSuccess ...
	TAIHETagHttpSuccess TAIHETag		= " _com_http_success"
	//TAIHETagHttpFailed ...
	TAIHETagHttpFailed TAIHETag			= " _com_http_failure"
	// TAIHETagRequestIn ...
	TAIHETagRequestIn TAIHETag			= " _com_request_in"
	//TAIHETagRequestOut ...
	TAIHETagRequestOut TAIHETag			= " _com_request_out"
	//TAIHETagUploadStart
	TAIHETagUploadStart TAIHETag		= "_com_upload_start"
	//TAIHETagUploadCreated
	TAIHETagUploadCreated TAIHETag		= "_com_upload_create"
	//TAIHETagChunkWriteStart
	TAIHETagChunkWriteStart TAIHETag	= "_com_upload_chunk_write_start"
	//TAIHETagChunkWriteComplete
	TAIHETagChunkWriteComplete			= "_com_upload_chunk_write_complete"
	//TAIHETag302
	TAIHETag302							= "_com_upload_302"
	//TAIHETagCreateMultipartUpload
	TAIHETagCreateMultipartUpload		= "_com_upload_multipart_create"
	//TAIHETagMethodIncome
	TAIHETagMethodIn					= "_com_upload_method_in"
	//TAIHETagMethodOut
	TAIHETagMethodOut					= "_com_upload_method_out"
)
