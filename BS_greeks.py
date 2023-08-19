import scipy.stats as scistat
import numpy as np

class BS():  
    '''
    Input parameters:
    S -> asset price
    K -> strike price
    r -> interest rate
    sigma -> volatility
    T -> time to maturity
    '''  

    def d1(self, S, K, r, sigma, T):
        return (np.log(S/K) + (r+sigma*sigma/2)*T)/(sigma*np.sqrt(T))

    def d2(self, S, K, r, sigma, T):
        return self.d1(S, K, r, sigma, T) - sigma*np.sqrt(T)
    
    def price(self, S, K, r, sigma, T, cp):
        if cp == 'c':
            return np.maximum(S - K, 0) if T==0 else S*scistat.norm.cdf(self.d1(S, K, r, sigma, T)) - K*np.exp(-r*T)*scistat.norm.cdf(self.d2(S, K, r, sigma, T))
        else:
            return np.maximum(K-S,0) if T==0 else K*np.exp(-r*T)*scistat.norm.cdf(-1*self.d2(S, K, r, sigma, T)) - S*scistat.norm.cdf(-1*self.d1(S, K, r, sigma, T))

    def delta(self, S, K, r, sigma, T, cp):
        if cp == 'c':
            return scistat.norm.cdf(self.d1(S, K, r, sigma, T))
        else:
            return scistat.norm.cdf(self.d1(S, K, r, sigma, T)) - 1

    def gamma(self, S, K, r, sigma, T, cp = 'c'):
        return scistat.norm.pdf(self.d1(S, K, r, sigma, T))/(S*sigma*np.sqrt(T))

    def vega(self, S, K, r, sigma, T, cp = 'c'):
        return S*scistat.norm.pdf(self.d1(S, K, r, sigma, T))*np.sqrt(T)

    def theta(self, S, K, r, sigma, T, cp):
        if cp == 'c':
            aux1 = -S*scistat.norm.pdf(self.d1(S, K, r, sigma, T))*sigma/(2*np.sqrt(T))
            aux2 = -r*K*np.exp(-r*T)*scistat.norm.cdf(self.d2(S, K, r, sigma, T))
            return aux1+aux2
        else:
            aux1 = -S*scistat.norm.pdf(self.d1(S, K, r, sigma, T))*sigma/(2*np.sqrt(T))
            aux2 = r*K*np.exp(-r*T)*scistat.norm.cdf(-1*self.d2(S, K, r, sigma, T))
            return aux1+aux2

    def rho(self, S, K, r, sigma, T, cp):
        if cp == 'c':
            return K*T*np.exp(-r*T)*scistat.norm.cdf(self.d2(S, K, r, sigma, T))
        else:
            return -K*T*np.exp(-r*T)*scistat.norm.cdf(-1*self.d2(S, K, r, sigma, T))

    def iv_by_newton_raphson(self, P, S, K, r, T, cp, tol=0.0001, max_iterations=100):
        '''
        :param P: Observed option price
        :param S: Asset price
        :param K: Strike Price
        :param T: Time to Maturity
        :param r: riskfree rate
        :param cp: call/put flag
        :param tol: error tolerance in result
        :param max_iterations: max iterations to update vol
        :return: implied volatility 
        '''
        ### assigning initial volatility estimate for input in Newton_rap procedure
        sigma = 0.5

        for i in range(max_iterations):

            ### calculate difference between blackscholes price and market price with
            ### iteratively updated volality estimate
            diff = self.price(S, K, r, sigma, T, cp) - P

            ###break if difference is less than specified tolerance level
            if abs(diff) < tol:
                break
            
            ### use newton rapshon to update the estimate
            sigma = sigma - diff / self.vega(S, K, r, sigma, T)

        return sigma

    def get_convexity_spot_shift(self, P, S, K, r, T, cp):
        return (self.iv_by_newton_raphson(P, S+1e-5, K, r, T, cp) - self.iv_by_newton_raphson(P, S-1e-5, K, r, T, cp)) / (2*1e-5)
    
    def get_convexity_strike_shift(self, P, S, K, r, T, cp):
        return (self.iv_by_newton_raphson(P, S, K+1e-5, r, T, cp) - self.iv_by_newton_raphson(P, S, K-1e-5, r, T, cp)) / (2*1e-5) * 1 #(-K/S)
    

    def all(self, S, K, r, sigma, T, cp = 'c'):
        return self.price(S, K, r, sigma, T, cp), self.delta(S, K, r, sigma, T, cp), \
                self.gamma(S, K, r, sigma, T, cp), self.vega(S, K, r, sigma, T, cp), \
                self.theta(S, K, r, sigma, T, cp), self.rho(S, K, r, sigma, T, cp)



if __name__ == "__main__":
    price = 18
    S = 100
    K = 115
    T = 1
    r = 0.05

    bs = BS()
    sigma_call = bs.iv_by_newton_raphson(price, S, K, r, T, 'c')
    sigma_put = bs.iv_by_newton_raphson(price, S, K, r, T, 'p')

    call_price = bs.price(S, K, r, sigma_call, T, 'c')
    put_price = bs.price(S, K, r, sigma_put, T, 'p')
    print("Call Option Price:", call_price)
    print("Put Option Price:", put_price)


    call_delta = bs.delta(S, K, r, sigma_call, T, 'c')
    put_delta = bs.delta(S, K, r, sigma_put, T, 'p')
    print("Call Option Delta:", call_delta)
    print("Put Option Delta:", put_delta)

    gamma = bs.gamma(S, K, r, sigma_call, T)
    print("Option Gamma:", gamma)

    vega = bs.vega(S, K, r, sigma_call, T)
    print("Option Vega:", vega)

    call_theta = bs.theta(S, K, r, sigma_call, T, 'c')
    put_theta = bs.theta(S, K, r, sigma_put, T, 'p')
    print("Call Option Theta:", call_theta)
    print("Put Option Theta:", put_theta)

    call_rho = bs.rho(S, K, r, sigma_call, T, 'c')
    put_rho = bs.rho(S, K, r, sigma_put, T, 'p')
    print("Call Option Rho:", call_rho)
    print("Put Option Rho:", put_rho)

    print()
    print()
    S=31016
    price=0.0464*S
    K=45000.0
    r=0.05
    T=+182/365.25
    sigma = 0.5492
    print("strike 45000")
    delta = bs.delta(S, K, r, sigma, T, 'c')
    vega = bs.vega(S, K, r, sigma, T)
    convexity_sp = bs.get_convexity_spot_shift(price, S, K, r, T, 'c')
    print("Convexity sp:", convexity_sp)
    convexity_st = bs.get_convexity_strike_shift(price, S, K, r, T, 'c')
    print("Convexity st:", convexity_st)
    print(f"vega={vega}")
    print(f"delta={delta}")
    print(f"numerical delta={(bs.price(S+1e-5, K, r, sigma, T, 'c') - bs.price(S-1e-5, K, r, sigma, T, 'c'))/(2*1e-5)}")
    print(f"smiled delta (spot shift) = {delta + vega/S*100 * convexity_sp}")
    print(f"smiled delta (strike shift) = {delta + vega/S*100 * convexity_st}")
    print(f"smiled delta (spot shift) = {delta + vega/100 * convexity_sp}")
    print(f"smiled delta (strike shift) = {delta + vega/100 * convexity_st}")

    print()
    S=31016
    price=0.0533*S
    K=26000.0
    r=0.05
    T=+182/365.25
    sigma = 0.4891
    print("strike 26000")
    delta = bs.delta(S, K, r, sigma, T, 'p')
    vega = bs.vega(S, K, r, sigma, T)
    convexity_sp = bs.get_convexity_spot_shift(price, S, K, r, T, 'p')
    print("Convexity sp:", convexity_sp)
    convexity_st = bs.get_convexity_strike_shift(price, S, K, r, T, 'p')
    print("Convexity st:", convexity_st)
    print(f"vega={vega}")
    print(f"delta={delta}")
    print(f"numerical delta={(bs.price(S+1e-5, K, r, sigma, T, 'p') - bs.price(S-1e-5, K, r, sigma, T, 'p'))/(2*1e-5)}")
    print(f"smiled delta (spot shift) = {delta + vega/S * convexity_sp}")
    print(f"smiled delta (strike shift) = {delta + vega/S * convexity_st}")
    print(f"smiled delta (spot shift) = {delta + vega/100 * convexity_sp}")
    print(f"smiled delta (strike shift) = {delta + vega/100 * convexity_st}")

    print()
    print()
    metrics = bs.all(S, K, r, sigma, T, cp='p')

    call_price, call_delta, call_gamma, call_vega, call_theta, call_rho = metrics
    print("Call Option Price:", call_price/S)
    print("Call Option Delta:", call_delta)
    print("Call Option Gamma:", call_gamma)
    print("Call Option Vega:", call_vega)
    print("Call Option Theta:", call_theta)
    print("Call Option Rho:", call_rho)