package vision;
public class ExceptionCode {
 
	private int errorCode = 0;
	private String errorMsg = "";
	private String errorSevr = "";
	private Object otherInfo = null;// extra information
	private Object request = null;//request information
	private Object response = null;//response list/array etc.
	private String actionType = "";
	
	public String getActionType() {
		return actionType;
	}
	public void setActionType(String actionType) {
		this.actionType = actionType;
	}
	public Object getRequest() {
		return request;
	}
	public void setRequest(Object request) {
		this.request = request;
	}
	public Object getResponse() {
		return response;
	}
	public void setResponse(Object response) {
		this.response = response;
	}
	public int getErrorCode() {
		return errorCode;
	}
	public void setErrorCode(int errorCode) {
		this.errorCode = errorCode;
	}
	public String getErrorMsg() {
		return errorMsg;
	}
	public void setErrorMsg(String errorMsg) {
		this.errorMsg = errorMsg;
	}
	public String getErrorSevr() {
		return errorSevr;
	}
	public void setErrorSevr(String errorSevr) {
		this.errorSevr = errorSevr;
	}
	public Object getOtherInfo() {
		return otherInfo;
	}
	public void setOtherInfo(Object otherInfo) {
		this.otherInfo = otherInfo;
	}
	
	
}
