package com.github.lisama569.resilientapigatewayjava.upstream;

/** 上游调用结果。 */
public class UpstreamResult {

    private final int status;
    private final String body;
    private final boolean duplicate;

    public UpstreamResult(int status, String body, boolean duplicate) {
        this.status = status;
        this.body = body;
        this.duplicate = duplicate;
    }

    public int getStatus() {
        return status;
    }

    public String getBody() {
        return body;
    }

    /** 是否命中幂等去重（即重复投递，未产生新副作用）。 */
    public boolean isDuplicate() {
        return duplicate;
    }
}
