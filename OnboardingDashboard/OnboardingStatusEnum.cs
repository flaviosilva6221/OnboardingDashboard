namespace OnboardingDashboard
{
    public enum OnboardingStatusEnum
    {
        Ongoing = 32001, // The onboarding process didn't finished
        InputEnded = 32002, // Waiting validation process, score ...
        Approved = 32003, // All validation process are finished and the Lead was approved to be a customer
        Rejected = 32004, // All validation process are finished and the Lead was refused to be a customer
        PendingApproval = 32005, // All validation process are finished, Lead was approved and the documentNumber was not in whitelist
        PendingAnalysis = 32006, // Pending analysis from fraudscore
        SelfieFailure = 32101, // Liveness was finished and failed, user must send a new selfie
        DocumentFailure = 32102, // Generic document was finished and failed, user must resend a document
        DocumentSelfieFailure = 32103, // Generic document + liveness was finished and failed, user must resend a document + selfie
    }
}
