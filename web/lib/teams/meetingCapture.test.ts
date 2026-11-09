import { expect, it } from "vitest";
import { meetingCapturePath, validateMeetingCaptureQuery } from "./meetingCapture";
it("accepts backend-supported cloud hosts but refuses unrelated and personal endpoints",()=>{
  expect(meetingCapturePath("https://teams.cloud.microsoft/meet/123")).toContain("join_url=");
  for (const url of ["https://teams.live.com/meet/123","https://teams.microsoft.com.evil/meet/123","http://teams.microsoft.com/meet/123","https://user@teams.microsoft.com/meet/123"])
    expect(()=>meetingCapturePath(url)).toThrow();
});

it("rejects mixed or incomplete meeting selectors rather than choosing one silently",()=>{
  for (const query of ["meeting_ticket=one&join_url=two","teams_meeting_id=a&teams_chat_id=b&meeting_ticket=c","teams_meeting_id=a","teams_chat_id=b","meeting_ticket=a&meeting_ticket=b","join_url="])
    expect(()=>validateMeetingCaptureQuery(new URLSearchParams(query))).toThrow();
  expect(()=>validateMeetingCaptureQuery(new URLSearchParams("teams_meeting_id=a&teams_chat_id=b"))).not.toThrow();
});
