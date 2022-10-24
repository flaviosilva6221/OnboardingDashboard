namespace OnboardingDashboard
{
    public class CustomerProfileTypeEnum : Enumeration
    {
        public static readonly CustomerProfileTypeEnum Conservative = new CustomerProfileTypeEnum(0, "Conservative");
        public static readonly CustomerProfileTypeEnum Moderate = new CustomerProfileTypeEnum(1, "Moderate");
        public static readonly CustomerProfileTypeEnum Aggressive = new CustomerProfileTypeEnum(2, "Aggressive");
        public static readonly CustomerProfileTypeEnum ERRO = new CustomerProfileTypeEnum(3, "ERRO");

        private CustomerProfileTypeEnum(int value, string name) : base(value, name)
        {
        }

        public CustomerProfileTypeEnum()
        {
        }
    }
}
