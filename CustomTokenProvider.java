import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.extensions.CustomTokenProviderAdaptee;
import java.util.Date;

public class CustomTokenProvider implements CustomTokenProviderAdaptee {
    private String accessToken;
    private Date expiresOn;

    @Override
    public void initialize(Configuration configuration, String accountName) {
        // Retrieve token and expiry from the configuration
        this.accessToken = configuration.get("fs.azure.account.custom.token");
        String expiryStr = configuration.get("fs.azure.account.custom.token.expiry");

        if (this.accessToken == null || expiryStr == null) {
            throw new IllegalArgumentException("Token or expiry time is not provided in the configuration.");
        }

        try {
            // Convert expiry to a Date object
            long expiryEpochMillis = Long.parseLong(expiryStr);
            this.expiresOn = new Date(expiryEpochMillis);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Expiry time is not a valid number: " + expiryStr, e);
        }

        System.out.println("Initializing CustomTokenProvider for account: " + accountName);
    }

    @Override
    public String getAccessToken() {
        return this.accessToken;
    }

    @Override
    public Date getExpiryTime() {
        return this.expiresOn;
    }
}
